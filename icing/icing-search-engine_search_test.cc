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
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/index/lite/term-id-hit-pair.h"
#include "icing/jni/jni-cache.h"
#include "icing/join/join-processor.h"
#include "icing/legacy/index/icing-filesystem.h"
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
#include "icing/result/result-state-manager.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/icu-data-file-helper.h"
#include "icing/util/snippet-helpers.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::DoubleEq;
using ::testing::DoubleNear;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Lt;
using ::testing::Ne;
using ::testing::SizeIs;

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

// This test is meant to cover all tests relating to IcingSearchEngine::Search
// and IcingSearchEngine::GetNextPage.
class IcingSearchEngineSearchTest : public ::testing::Test {
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

constexpr double kEps = 0.000001;

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_document_store_namespace_id_fingerprint(true);
  icing_options.set_use_new_qualified_id_join_index(true);
  icing_options.set_enable_schema_database(true);
  icing_options.set_enable_embedding_index(true);
  icing_options.set_enable_scorable_properties(true);
  icing_options.set_enable_embedding_quantization(true);
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

SchemaProto CreatePersonAndEmailSchema() {
  return SchemaBuilder()
      .AddType(SchemaTypeConfigBuilder()
                   .SetType("Person")
                   .AddProperty(PropertyConfigBuilder()
                                    .SetName("name")
                                    .SetDataTypeString(TERM_MATCH_PREFIX,
                                                       TOKENIZER_PLAIN)
                                    .SetCardinality(CARDINALITY_OPTIONAL))
                   .AddProperty(PropertyConfigBuilder()
                                    .SetName("emailAddress")
                                    .SetDataTypeString(TERM_MATCH_PREFIX,
                                                       TOKENIZER_PLAIN)
                                    .SetCardinality(CARDINALITY_OPTIONAL)))
      .AddType(
          SchemaTypeConfigBuilder()
              .SetType("Email")
              .AddProperty(
                  PropertyConfigBuilder()
                      .SetName("body")
                      .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                      .SetCardinality(CARDINALITY_OPTIONAL))
              .AddProperty(
                  PropertyConfigBuilder()
                      .SetName("subject")
                      .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                      .SetCardinality(CARDINALITY_OPTIONAL))
              .AddProperty(PropertyConfigBuilder()
                               .SetName("sender")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL)))
      .Build();
}

ScoringSpecProto GetDefaultScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

UsageReport CreateUsageReport(std::string name_space, std::string uri,
                              int64_t timestamp_ms,
                              UsageReport::UsageType usage_type) {
  UsageReport usage_report;
  usage_report.set_document_namespace(name_space);
  usage_report.set_document_uri(uri);
  usage_report.set_usage_timestamp_ms(timestamp_ms);
  usage_report.set_usage_type(usage_type);
  return usage_report;
}

std::vector<std::string> GetUrisFromSearchResults(
    SearchResultProto& search_result_proto) {
  std::vector<std::string> result_uris;
  result_uris.reserve(search_result_proto.results_size());
  for (int i = 0; i < search_result_proto.results_size(); i++) {
    result_uris.push_back(
        search_result_proto.mutable_results(i)->document().uri());
  }
  return result_uris;
}

std::vector<int32_t> GetScoresFromSearchResults(
    SearchResultProto& search_result_proto) {
  std::vector<int32_t> result_scores;
  for (int i = 0; i < search_result_proto.results_size(); i++) {
    result_scores.push_back(search_result_proto.results(i).score());
  }
  return result_scores;
}

void AddSchemaTypeAliasMap(ScoringSpecProto* scoring_spec,
                           const std::string& alias_schema_type,
                           const std::vector<std::string>& schema_types) {
  SchemaTypeAliasMapProto* alias_map_proto =
      scoring_spec->add_schema_type_alias_map_protos();
  alias_map_proto->set_alias_schema_type(alias_schema_type);
  alias_map_proto->mutable_schema_types()->Add(schema_types.begin(),
                                               schema_types.end());
}

TEST_F(IcingSearchEngineSearchTest, SearchReturnsValidResults) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document_one = CreateMessageDocument("namespace", "uri1");
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two = CreateMessageDocument("namespace", "uri2");
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(1);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(1);

  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));

  const DocumentProto& document = results.results(0).document();
  EXPECT_THAT(document, EqualsProto(document_two));

  const SnippetProto& snippet = results.results(0).snippet();
  EXPECT_THAT(snippet.entries(), SizeIs(1));
  EXPECT_THAT(snippet.entries(0).property_name(), Eq("body"));
  std::string_view content =
      GetString(&document, snippet.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, snippet.entries(0)),
              ElementsAre("message body"));
  EXPECT_THAT(GetMatches(content, snippet.entries(0)), ElementsAre("message"));

  EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));
  EXPECT_THAT(results.results(1).snippet().entries(), IsEmpty());

  search_spec.set_query("foo");

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchReturnsScoresDocumentScore) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document_one = CreateMessageDocument("namespace", "uri1");
  document_one.set_score(93);
  document_one.set_creation_timestamp_ms(10000);
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two = CreateMessageDocument("namespace", "uri2");
  document_two.set_score(15);
  document_two.set_creation_timestamp_ms(12000);
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  // Rank by DOCUMENT_SCORE and ensure that the score field is populated with
  // document score.
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));

  EXPECT_THAT(results.results(0).document(), EqualsProto(document_one));
  EXPECT_THAT(results.results(0).score(), 93);
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_two));
  EXPECT_THAT(results.results(1).score(), 15);
}

TEST_F(IcingSearchEngineSearchTest, SearchReturnsScoresCreationTimestamp) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document_one = CreateMessageDocument("namespace", "uri1");
  document_one.set_score(93);
  document_one.set_creation_timestamp_ms(10000);
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two = CreateMessageDocument("namespace", "uri2");
  document_two.set_score(15);
  document_two.set_creation_timestamp_ms(12000);
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  // Rank by CREATION_TS and ensure that the score field is populated with
  // creation ts.
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);

  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));

  EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));
  EXPECT_THAT(results.results(0).score(), 12000);
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));
  EXPECT_THAT(results.results(1).score(), 10000);
}

TEST_F(IcingSearchEngineSearchTest, SearchReturnsOneResult) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document_one = CreateMessageDocument("namespace", "uri1");
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two = CreateMessageDocument("namespace", "uri2");
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

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
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  EXPECT_THAT(search_result_proto.query_stats().latency_ms(), Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().document_retrieval_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().lock_acquisition_latency_ms(),
              Eq(1000));
  // TODO(b/305098009): deprecate search-related flat fields in query_stats.
  EXPECT_THAT(search_result_proto.query_stats().parse_query_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().scoring_latency_ms(), Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().ranking_latency_ms(), Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .parse_query_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .scoring_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_documents_scored(),
              Eq(2));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_lite_index(),
              Eq(2));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_main_index(),
              Eq(0));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_integer_index(),
              Eq(0));

  // The token is a random number so we don't verify it.
  expected_search_result_proto.set_next_page_token(
      search_result_proto.next_page_token());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchReturnsOneResult_readOnlyFalse) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document_one = CreateMessageDocument("namespace", "uri1");
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two = CreateMessageDocument("namespace", "uri2");
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  search_spec.set_use_read_only_search(false);

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document_two;

  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  EXPECT_THAT(search_result_proto.query_stats().latency_ms(), Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().document_retrieval_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().lock_acquisition_latency_ms(),
              Eq(1000));
  // TODO(b/305098009): deprecate search-related flat fields in query_stats.
  EXPECT_THAT(search_result_proto.query_stats().parse_query_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().scoring_latency_ms(), Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().ranking_latency_ms(), Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .parse_query_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .scoring_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_documents_scored(),
              Eq(2));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_lite_index(),
              Eq(2));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_main_index(),
              Eq(0));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_integer_index(),
              Eq(0));

  // The token is a random number so we don't verify it.
  expected_search_result_proto.set_next_page_token(
      search_result_proto.next_page_token());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchZeroResultLimitReturnsEmptyResults) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(0);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchZeroResultLimitReturnsEmptyResults_readOnlyFalse) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  search_spec.set_use_read_only_search(false);

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(0);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithNumToScore) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document_one = CreateMessageDocument("namespace", "uri1");
  document_one.set_score(10);
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two = CreateMessageDocument("namespace", "uri2");
  document_two.set_score(5);
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(10);
  result_spec.set_num_to_score(10);

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();

  SearchResultProto expected_search_result_proto1;
  expected_search_result_proto1.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto1.mutable_results()->Add()->mutable_document() =
      document_one;
  *expected_search_result_proto1.mutable_results()->Add()->mutable_document() =
      document_two;

  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto1));

  result_spec.set_num_to_score(1);
  // By setting num_to_score = 1, only document_two will be scored, ranked, and
  // returned.
  // - num_to_score cutoff is only affected by the reading order from posting
  //   list. IOW, since we read posting lists in doc id descending order,
  //   ScoringProcessor scores documents with higher doc ids first and cuts off
  //   if exceeding num_to_score.
  // - Therefore, even though document_one has higher score, ScoringProcessor
  //   still skips document_one, because posting list reads document_two first
  //   and ScoringProcessor stops after document_two given that total # of
  //   scored document has already reached num_to_score.
  SearchResultProto expected_search_result_google::protobuf;
  expected_search_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_google::protobuf.mutable_results()->Add()->mutable_document() =
      document_two;

  search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_google::protobuf));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchNegativeResultLimitReturnsInvalidArgument) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

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
  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchNegativeResultLimitReturnsInvalidArgument_readOnlyFalse) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  search_spec.set_use_read_only_search(false);

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(-5);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "ResultSpecProto.num_per_page cannot be negative.");
  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchNonPositivePageTotalBytesLimitReturnsInvalidArgument) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  ResultSpecProto result_spec;
  result_spec.set_num_total_bytes_per_page_threshold(-1);

  SearchResultProto actual_results1 =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results1.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));

  result_spec.set_num_total_bytes_per_page_threshold(0);
  SearchResultProto actual_results2 =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results2.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchNegativeMaxJoinedChildrenPerParentReturnsInvalidArgument) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  ResultSpecProto result_spec;
  result_spec.set_max_joined_children_per_parent_to_return(-1);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "ResultSpecProto.max_joined_children_per_parent_to_return cannot be "
      "negative.");
  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchNonPositiveNumToScoreReturnsInvalidArgument) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  ResultSpecProto result_spec;
  result_spec.set_num_to_score(-1);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "ResultSpecProto.num_to_score cannot be non-positive.");

  SearchResultProto actual_results1 =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results1, EqualsSearchResultIgnoreStatsAndScores(
                                   expected_search_result_proto));

  result_spec.set_num_to_score(0);
  SearchResultProto actual_results2 =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(actual_results2, EqualsSearchResultIgnoreStatsAndScores(
                                   expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithPersistenceReturnsValidResults) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Set the schema up beforehand.
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    // Schema will be persisted to disk when icing goes out of scope.
  }

  {
    // Ensure that icing initializes the schema and section_manager
    // properly from the pre-existing file.
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(CreateMessageDocument("namespace", "uri")).status(),
                ProtoIsOk());
    // The index and document store will be persisted to disk when icing goes
    // out of scope.
  }

  {
    // Ensure that the index is brought back up without problems and we
    // can query for the content that we expect.
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::PREFIX);
    search_spec.set_query("message");

    SearchResultProto expected_search_result_proto;
    expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
        CreateMessageDocument("namespace", "uri");

    SearchResultProto actual_results =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                    expected_search_result_proto));

    search_spec.set_query("foo");

    SearchResultProto empty_result;
    empty_result.mutable_status()->set_code(StatusProto::OK);
    actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                  ResultSpecProto::default_instance());
    EXPECT_THAT(actual_results,
                EqualsSearchResultIgnoreStatsAndScores(empty_result));
  }
}

TEST_F(IcingSearchEngineSearchTest, SearchShouldReturnEmpty) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  // Empty result, no next-page token
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);

  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  EXPECT_THAT(search_result_proto.query_stats().latency_ms(), Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().document_retrieval_latency_ms(),
              Eq(0));
  EXPECT_THAT(search_result_proto.query_stats().lock_acquisition_latency_ms(),
              Eq(1000));
  // TODO(b/305098009): deprecate search-related flat fields in query_stats.
  EXPECT_THAT(search_result_proto.query_stats().parse_query_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().scoring_latency_ms(), Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats().ranking_latency_ms(), Eq(0));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .parse_query_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .scoring_latency_ms(),
              Eq(1000));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_documents_scored(),
              Eq(0));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_lite_index(),
              Eq(0));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_main_index(),
              Eq(0));
  EXPECT_THAT(search_result_proto.query_stats()
                  .parent_search_stats()
                  .num_fetched_hits_integer_index(),
              Eq(0));

  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchShouldReturnMultiplePages) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates and inserts 5 documents
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");
  DocumentProto document4 = CreateMessageDocument("namespace", "uri4");
  DocumentProto document5 = CreateMessageDocument("namespace", "uri5");
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document4).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document5).status(), ProtoIsOk());

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
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

  // Second page, 2 results
  expected_search_result_proto.clear_results();
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  search_result_proto = icing.GetNextPage(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

  // Third page, 1 result
  expected_search_result_proto.clear_results();
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;
  // Because there are no more results, we should not return the next page
  // token.
  expected_search_result_proto.clear_next_page_token();
  search_result_proto = icing.GetNextPage(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

  // No more results
  expected_search_result_proto.clear_results();
  search_result_proto = icing.GetNextPage(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithNoScoringShouldReturnMultiplePages) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates and inserts 5 documents
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");
  DocumentProto document4 = CreateMessageDocument("namespace", "uri4");
  DocumentProto document5 = CreateMessageDocument("namespace", "uri5");
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document4).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document5).status(), ProtoIsOk());

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
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

  // Second page, 2 results
  expected_search_result_proto.clear_results();
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  search_result_proto = icing.GetNextPage(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

  // Third page, 1 result
  expected_search_result_proto.clear_results();
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;
  // Because there are no more results, we should not return the next page
  // token.
  expected_search_result_proto.clear_next_page_token();
  search_result_proto = icing.GetNextPage(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

  // No more results
  expected_search_result_proto.clear_results();
  search_result_proto = icing.GetNextPage(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithUnknownEnabledFeatureShouldReturnError) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  search_spec.add_enabled_features("BAD_FEATURE");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest, ShouldReturnMultiplePagesWithSnippets) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates and inserts 5 documents
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");
  DocumentProto document4 = CreateMessageDocument("namespace", "uri4");
  DocumentProto document5 = CreateMessageDocument("namespace", "uri5");
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document4).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document5).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(2);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(1);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(3);

  // Searches and gets the first page, 2 results with 2 snippets
  SearchResultProto search_result =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  ASSERT_THAT(search_result.results(), SizeIs(2));
  ASSERT_THAT(search_result.next_page_token(), Gt(kInvalidNextPageToken));

  const DocumentProto& document_result_1 = search_result.results(0).document();
  EXPECT_THAT(document_result_1, EqualsProto(document5));
  const SnippetProto& snippet_result_1 = search_result.results(0).snippet();
  EXPECT_THAT(snippet_result_1.entries(), SizeIs(1));
  EXPECT_THAT(snippet_result_1.entries(0).property_name(), Eq("body"));
  std::string_view content = GetString(
      &document_result_1, snippet_result_1.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, snippet_result_1.entries(0)),
              ElementsAre("message body"));
  EXPECT_THAT(GetMatches(content, snippet_result_1.entries(0)),
              ElementsAre("message"));

  const DocumentProto& document_result_2 = search_result.results(1).document();
  EXPECT_THAT(document_result_2, EqualsProto(document4));
  const SnippetProto& snippet_result_2 = search_result.results(1).snippet();
  EXPECT_THAT(snippet_result_2.entries(0).property_name(), Eq("body"));
  content = GetString(&document_result_2,
                      snippet_result_2.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, snippet_result_2.entries(0)),
              ElementsAre("message body"));
  EXPECT_THAT(GetMatches(content, snippet_result_2.entries(0)),
              ElementsAre("message"));

  // Second page, 2 result with 1 snippet
  search_result = icing.GetNextPage(search_result.next_page_token());
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  ASSERT_THAT(search_result.results(), SizeIs(2));
  ASSERT_THAT(search_result.next_page_token(), Gt(kInvalidNextPageToken));

  const DocumentProto& document_result_3 = search_result.results(0).document();
  EXPECT_THAT(document_result_3, EqualsProto(document3));
  const SnippetProto& snippet_result_3 = search_result.results(0).snippet();
  EXPECT_THAT(snippet_result_3.entries(0).property_name(), Eq("body"));
  content = GetString(&document_result_3,
                      snippet_result_3.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, snippet_result_3.entries(0)),
              ElementsAre("message body"));
  EXPECT_THAT(GetMatches(content, snippet_result_3.entries(0)),
              ElementsAre("message"));

  EXPECT_THAT(search_result.results(1).document(), EqualsProto(document2));
  EXPECT_THAT(search_result.results(1).snippet().entries(), IsEmpty());

  // Third page, 1 result with 0 snippets
  search_result = icing.GetNextPage(search_result.next_page_token());
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  ASSERT_THAT(search_result.results(), SizeIs(1));
  ASSERT_THAT(search_result.next_page_token(), Eq(kInvalidNextPageToken));

  EXPECT_THAT(search_result.results(0).document(), EqualsProto(document1));
  EXPECT_THAT(search_result.results(0).snippet().entries(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, ShouldInvalidateNextPageToken) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

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
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
  // Now document1 is still to be fetched.

  // Invalidates token
  icing.InvalidateNextPageToken(next_page_token);

  // Tries to fetch the second page, no result since it's invalidated
  expected_search_result_proto.clear_results();
  expected_search_result_proto.clear_next_page_token();
  search_result_proto = icing.GetNextPage(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchIncludesDocumentsBeforeTtl) {
  SchemaProto schema;
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
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());

  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Check that the document is returned as part of search results
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchDoesntIncludeDocumentsPastTtl) {
  SchemaProto schema;
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
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());

  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Check that the document is not returned as part of search results
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWorksAfterSchemaTypesCompatiblyModified) {
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

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(message_document).status(), ProtoIsOk());

  // Make sure we can search for message document
  SearchSpecProto search_spec;
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);

  // The message isn't indexed, so we get nothing
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

  // With just the schema type filter, we can search for the message
  search_spec.Clear();
  search_spec.add_schema_type_filters("message");

  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      message_document;

  search_result_proto = icing.Search(search_spec, GetDefaultScoringSpec(),
                                     ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

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
  property->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::PREFIX);
  property->mutable_string_indexing_config()->set_tokenizer_type(
      StringIndexingConfig::TokenizerType::PLAIN);

  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  search_spec.Clear();
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.add_schema_type_filters("message");

  // We can still search for the message document
  search_result_proto = icing.Search(search_spec, GetDefaultScoringSpec(),
                                     ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchResultShouldBeRankedByDocumentScore) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

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
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());

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
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchWorksForNestedSubtypeDocument) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Artist")
                       .AddParentType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder().SetType("Company").AddProperty(
              PropertyConfigBuilder()
                  .SetName("employee")
                  .SetDataTypeDocument("Person",
                                       /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // Create a company with a person and an artist.
  DocumentProto document_company =
      DocumentBuilder()
          .SetKey("namespace", "uri")
          .SetCreationTimestampMs(1000)
          .SetSchema("Company")
          .AddDocumentProperty("employee",
                               DocumentBuilder()
                                   .SetKey("namespace", "uri1")
                                   .SetCreationTimestampMs(1000)
                                   .SetSchema("Person")
                                   .AddStringProperty("name", "name_person")
                                   .Build(),
                               DocumentBuilder()
                                   .SetKey("namespace", "uri2")
                                   .SetCreationTimestampMs(1000)
                                   .SetSchema("Artist")
                                   .AddStringProperty("name", "name_artist")
                                   .AddStringProperty("emailAddress", "email")
                                   .Build())
          .Build();
  ASSERT_THAT(icing.Put(document_company).status(), ProtoIsOk());

  SearchResultProto company_search_result_proto;
  company_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *company_search_result_proto.mutable_results()->Add()->mutable_document() =
      document_company;

  SearchResultProto empty_search_result_proto;
  empty_search_result_proto.mutable_status()->set_code(StatusProto::OK);

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  // "name_person" should match the company.
  search_spec.set_query("name_person");
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       company_search_result_proto));

  // "name_artist" should match the company.
  search_spec.set_query("name_artist");
  search_result_proto = icing.Search(search_spec, GetDefaultScoringSpec(),
                                     ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       company_search_result_proto));

  // "email" should not match the company even though the artist has a matched
  // property. This is because the "employee" property is defined as Person
  // type, and indexing on document properties should be based on defined types,
  // instead of subtypes.
  search_spec.set_query("email");
  search_result_proto = icing.Search(search_spec, GetDefaultScoringSpec(),
                                     ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       empty_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchShouldAllowNoScoring) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

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
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

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
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultShouldBeRankedByCreationTimestamp) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

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
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

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
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchResultShouldBeRankedByUsageCount) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 3 test documents
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // Intentionally inserts the documents in a different order to eliminate the
  // possibility that the following results are sorted in the default reverse
  // insertion order.
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // Report usage for doc3 twice and doc2 once. The order will be doc3 > doc2 >
  // doc1 when ranked by USAGE_TYPE1_COUNT.
  UsageReport usage_report_doc3 = CreateUsageReport(
      /*name_space=*/"namespace", /*uri=*/"uri/3", /*timestamp_ms=*/0,
      UsageReport::USAGE_TYPE1);
  UsageReport usage_report_doc2 = CreateUsageReport(
      /*name_space=*/"namespace", /*uri=*/"uri/2", /*timestamp_ms=*/0,
      UsageReport::USAGE_TYPE1);
  ASSERT_THAT(icing.ReportUsage(usage_report_doc3).status(), ProtoIsOk());
  ASSERT_THAT(icing.ReportUsage(usage_report_doc3).status(), ProtoIsOk());
  ASSERT_THAT(icing.ReportUsage(usage_report_doc2).status(), ProtoIsOk());

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // Result should be in descending USAGE_TYPE1_COUNT order
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::USAGE_TYPE1_COUNT);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultShouldHaveDefaultOrderWithoutUsageCounts) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 3 test documents
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // None of the documents have usage reports. Result should be in the default
  // reverse insertion order.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::USAGE_TYPE1_COUNT);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultShouldBeRankedByUsageTimestamp) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 3 test documents
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // Intentionally inserts the documents in a different order to eliminate the
  // possibility that the following results are sorted in the default reverse
  // insertion order.
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // Report usage for doc2 and doc3. The order will be doc3 > doc2 > doc1 when
  // ranked by USAGE_TYPE1_LAST_USED_TIMESTAMP.
  UsageReport usage_report_doc2 = CreateUsageReport(
      /*name_space=*/"namespace", /*uri=*/"uri/2", /*timestamp_ms=*/1000,
      UsageReport::USAGE_TYPE1);
  UsageReport usage_report_doc3 = CreateUsageReport(
      /*name_space=*/"namespace", /*uri=*/"uri/3", /*timestamp_ms=*/5000,
      UsageReport::USAGE_TYPE1);
  ASSERT_THAT(icing.ReportUsage(usage_report_doc2).status(), ProtoIsOk());
  ASSERT_THAT(icing.ReportUsage(usage_report_doc3).status(), ProtoIsOk());

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // Result should be in descending USAGE_TYPE1_LAST_USED_TIMESTAMP order
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::USAGE_TYPE1_LAST_USED_TIMESTAMP);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, Bm25fRelevanceScoringOneNamespace) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());

  // Create and index documents in namespace "namespace1".
  DocumentProto document = CreateEmailDocument(
      "namespace1", "namespace1/uri0", /*score=*/10, "sushi belmont",
      "fresh fish. inexpensive. good sushi.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri1", /*score=*/13, "peacock koriander",
      "indian food. buffet. spicy food. kadai chicken.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri2", /*score=*/4,
                                 "panda express",
                                 "chinese food. cheap. inexpensive. kung pao.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri3", /*score=*/23,
                                 "speederia pizza",
                                 "thin-crust pizza. good and fast.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri4", /*score=*/8,
                                 "whole foods",
                                 "salads. pizza. organic food. expensive.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri5", /*score=*/18, "peets coffee",
      "espresso. decaf. brewed coffee. whole beans. excellent coffee.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri6", /*score=*/4, "costco",
      "bulk. cheap whole beans. frozen fish. food samples.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri7", /*score=*/4,
                                 "starbucks coffee",
                                 "habit. birthday rewards. good coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("coffee OR food");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());

  // Result should be in descending score order
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());
  // Both doc5 and doc7 have "coffee" in name and text sections.
  // However, doc5 has more matches in the text section.
  // Documents with "food" are ranked lower as the term "food" is commonly
  // present in this corpus, and thus, has a lower IDF.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("namespace1/uri5",    // 'coffee' 3 times
                          "namespace1/uri7",    // 'coffee' 2 times
                          "namespace1/uri1",    // 'food' 2 times
                          "namespace1/uri4",    // 'food' 2 times
                          "namespace1/uri2",    // 'food' 1 time
                          "namespace1/uri6"));  // 'food' 1 time
}

TEST_F(IcingSearchEngineSearchTest, Bm25fRelevanceScoringOneNamespaceAdvanced) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());

  // Create and index documents in namespace "namespace1".
  DocumentProto document = CreateEmailDocument(
      "namespace1", "namespace1/uri0", /*score=*/10, "sushi belmont",
      "fresh fish. inexpensive. good sushi.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri1", /*score=*/13, "peacock koriander",
      "indian food. buffet. spicy food. kadai chicken.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri2", /*score=*/4,
                                 "panda express",
                                 "chinese food. cheap. inexpensive. kung pao.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri3", /*score=*/23,
                                 "speederia pizza",
                                 "thin-crust pizza. good and fast.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri4", /*score=*/8,
                                 "whole foods",
                                 "salads. pizza. organic food. expensive.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri5", /*score=*/18, "peets coffee",
      "espresso. decaf. brewed coffee. whole beans. excellent coffee.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri6", /*score=*/4, "costco",
      "bulk. cheap whole beans. frozen fish. food samples.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri7", /*score=*/4,
                                 "starbucks coffee",
                                 "habit. birthday rewards. good coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("coffee OR food");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_advanced_scoring_expression("this.relevanceScore() * 2 + 1");
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());

  // Result should be in descending score order
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());
  // Both doc5 and doc7 have "coffee" in name and text sections.
  // However, doc5 has more matches in the text section.
  // Documents with "food" are ranked lower as the term "food" is commonly
  // present in this corpus, and thus, has a lower IDF.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("namespace1/uri5",    // 'coffee' 3 times
                          "namespace1/uri7",    // 'coffee' 2 times
                          "namespace1/uri1",    // 'food' 2 times
                          "namespace1/uri4",    // 'food' 2 times
                          "namespace1/uri2",    // 'food' 1 time
                          "namespace1/uri6"));  // 'food' 1 time
}

TEST_F(IcingSearchEngineSearchTest,
       Bm25fRelevanceScoringOneNamespaceNotOperator) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());

  // Create and index documents in namespace "namespace1".
  DocumentProto document = CreateEmailDocument(
      "namespace1", "namespace1/uri0", /*score=*/10, "sushi belmont",
      "fresh fish. inexpensive. good sushi.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri1", /*score=*/13, "peacock koriander",
      "indian food. buffet. spicy food. kadai chicken.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri2", /*score=*/4,
                                 "panda express",
                                 "chinese food. cheap. inexpensive. kung pao.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri3", /*score=*/23, "speederia pizza",
      "thin-crust pizza. good and fast. nice coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri4", /*score=*/8,
                                 "whole foods",
                                 "salads. pizza. organic food. expensive.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri5", /*score=*/18, "peets coffee",
      "espresso. decaf. brewed coffee. whole beans. excellent coffee.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri6", /*score=*/4, "costco",
      "bulk. cheap whole beans. frozen fish. food samples.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri7", /*score=*/4,
                                 "starbucks coffee",
                                 "habit. birthday rewards. good coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("coffee -starbucks");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());

  // Result should be in descending score order
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(
      GetUrisFromSearchResults(search_result_proto),
      ElementsAre("namespace1/uri5",    // 'coffee' 3 times, 'starbucks' 0 times
                  "namespace1/uri3"));  // 'coffee' 1 times, 'starbucks' 0 times
}

TEST_F(IcingSearchEngineSearchTest,
       Bm25fRelevanceScoringOneNamespaceSectionRestrict) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());

  // Create and index documents in namespace "namespace1".
  DocumentProto document = CreateEmailDocument(
      "namespace1", "namespace1/uri0", /*score=*/10, "sushi belmont",
      "fresh fish. inexpensive. good sushi.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri1", /*score=*/13, "peacock koriander",
      "indian food. buffet. spicy food. kadai chicken.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri2", /*score=*/4,
                                 "panda express",
                                 "chinese food. cheap. inexpensive. kung pao.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri3", /*score=*/23,
                                 "speederia pizza",
                                 "thin-crust pizza. good and fast.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri4", /*score=*/8,
                                 "whole foods",
                                 "salads. pizza. organic food. expensive.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document =
      CreateEmailDocument("namespace1", "namespace1/uri5", /*score=*/18,
                          "peets coffee, best coffee",
                          "espresso. decaf. whole beans. excellent coffee.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri6", /*score=*/4, "costco",
      "bulk. cheap whole beans. frozen fish. food samples.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri7", /*score=*/4, "starbucks",
      "habit. birthday rewards. good coffee. brewed coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("subject:coffee OR body:food");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());

  // Result should be in descending score order
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());
  // The term frequencies of "coffee" and "food" are calculated respectively
  // from the subject section and the body section.
  // Documents with "food" are ranked lower as the term "food" is commonly
  // present in this corpus, and thus, has a lower IDF.
  EXPECT_THAT(
      GetUrisFromSearchResults(search_result_proto),
      ElementsAre("namespace1/uri5",    // 'coffee' 2 times in section subject
                  "namespace1/uri1",    // 'food' 2 times in section body
                  "namespace1/uri4",    // 'food' 2 times in section body
                  "namespace1/uri2",    // 'food' 1 time in section body
                  "namespace1/uri6"));  // 'food' 1 time in section body
}

TEST_F(IcingSearchEngineSearchTest, Bm25fRelevanceScoringTwoNamespaces) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());

  // Create and index documents in namespace "namespace1".
  DocumentProto document = CreateEmailDocument(
      "namespace1", "namespace1/uri0", /*score=*/10, "sushi belmont",
      "fresh fish. inexpensive. good sushi.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri1", /*score=*/13, "peacock koriander",
      "indian food. buffet. spicy food. kadai chicken.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri2", /*score=*/4,
                                 "panda express",
                                 "chinese food. cheap. inexpensive. kung pao.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri3", /*score=*/23,
                                 "speederia pizza",
                                 "thin-crust pizza. good and fast.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri4", /*score=*/8,
                                 "whole foods",
                                 "salads. pizza. organic food. expensive.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri5", /*score=*/18, "peets coffee",
      "espresso. decaf. brewed coffee. whole beans. excellent coffee.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri6", /*score=*/4, "costco",
      "bulk. cheap whole beans. frozen fish. food samples.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri7", /*score=*/4,
                                 "starbucks coffee",
                                 "habit. birthday rewards. good coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Create and index documents in namespace "namespace2".
  document = CreateEmailDocument("namespace2", "namespace2/uri0", /*score=*/10,
                                 "sushi belmont",
                                 "fresh fish. inexpensive. good sushi.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace2", "namespace2/uri1", /*score=*/13, "peacock koriander",
      "indian food. buffet. spicy food. kadai chicken.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace2", "namespace2/uri2", /*score=*/4,
                                 "panda express",
                                 "chinese food. cheap. inexpensive. kung pao.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace2", "namespace2/uri3", /*score=*/23,
                                 "speederia pizza",
                                 "thin-crust pizza. good and fast.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace2", "namespace2/uri4", /*score=*/8,
                                 "whole foods",
                                 "salads. pizza. organic food. expensive.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace2", "namespace2/uri5", /*score=*/18, "peets coffee",
      "espresso. decaf. brewed coffee. whole beans. excellent coffee.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace2", "namespace2/uri6", /*score=*/4, "costco",
      "bulk. cheap whole beans. frozen fish. food samples.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace2", "namespace2/uri7", /*score=*/4,
                                 "starbucks coffee", "good coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("coffee OR food");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE);
  ResultSpecProto result_spec_proto;
  result_spec_proto.set_num_per_page(16);
  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec_proto);

  // Result should be in descending score order
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());
  // The two corpora have the same documents except for document 7, which in
  // "namespace2" is much shorter than the average dcoument length, so it is
  // boosted.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("namespace2/uri7",    // 'coffee' 2 times, short doc
                          "namespace1/uri5",    // 'coffee' 3 times
                          "namespace2/uri5",    // 'coffee' 3 times
                          "namespace1/uri7",    // 'coffee' 2 times
                          "namespace1/uri1",    // 'food' 2 times
                          "namespace2/uri1",    // 'food' 2 times
                          "namespace1/uri4",    // 'food' 2 times
                          "namespace2/uri4",    // 'food' 2 times
                          "namespace1/uri2",    // 'food' 1 time
                          "namespace2/uri2",    // 'food' 1 time
                          "namespace1/uri6",    // 'food' 1 time
                          "namespace2/uri6"));  // 'food' 1 time
}

TEST_F(IcingSearchEngineSearchTest, Bm25fRelevanceScoringWithNamespaceFilter) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());

  // Create and index documents in namespace "namespace1".
  DocumentProto document = CreateEmailDocument(
      "namespace1", "namespace1/uri0", /*score=*/10, "sushi belmont",
      "fresh fish. inexpensive. good sushi.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri1", /*score=*/13, "peacock koriander",
      "indian food. buffet. spicy food. kadai chicken.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri2", /*score=*/4,
                                 "panda express",
                                 "chinese food. cheap. inexpensive. kung pao.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri3", /*score=*/23,
                                 "speederia pizza",
                                 "thin-crust pizza. good and fast.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri4", /*score=*/8,
                                 "whole foods",
                                 "salads. pizza. organic food. expensive.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri5", /*score=*/18, "peets coffee",
      "espresso. decaf. brewed coffee. whole beans. excellent coffee.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace1", "namespace1/uri6", /*score=*/4, "costco",
      "bulk. cheap whole beans. frozen fish. food samples.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace1", "namespace1/uri7", /*score=*/4,
                                 "starbucks coffee",
                                 "habit. birthday rewards. good coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Create and index documents in namespace "namespace2".
  document = CreateEmailDocument("namespace2", "namespace2/uri0", /*score=*/10,
                                 "sushi belmont",
                                 "fresh fish. inexpensive. good sushi.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace2", "namespace2/uri1", /*score=*/13, "peacock koriander",
      "indian food. buffet. spicy food. kadai chicken.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace2", "namespace2/uri2", /*score=*/4,
                                 "panda express",
                                 "chinese food. cheap. inexpensive. kung pao.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace2", "namespace2/uri3", /*score=*/23,
                                 "speederia pizza",
                                 "thin-crust pizza. good and fast.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace2", "namespace2/uri4", /*score=*/8,
                                 "whole foods",
                                 "salads. pizza. organic food. expensive.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace2", "namespace2/uri5", /*score=*/18, "peets coffee",
      "espresso. decaf. brewed coffee. whole beans. excellent coffee.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument(
      "namespace2", "namespace2/uri6", /*score=*/4, "costco",
      "bulk. cheap whole beans. frozen fish. food samples.");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  document = CreateEmailDocument("namespace2", "namespace2/uri7", /*score=*/4,
                                 "starbucks coffee", "good coffee");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("coffee OR food");

  // Now query only corpus 2
  search_spec.add_namespace_filters("namespace2");
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  search_result_proto = icing.Search(search_spec, scoring_spec,
                                     ResultSpecProto::default_instance());

  // Result from namespace "namespace2" should be in descending score order
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());
  // Both doc5 and doc7 have "coffee" in name and text sections.
  // Even though doc5 has more matches in the text section, doc7's length is
  // much shorter than the average corpus's length, so it's being boosted.
  // Documents with "food" are ranked lower as the term "food" is commonly
  // present in this corpus, and thus, has a lower IDF.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("namespace2/uri7",    // 'coffee' 2 times, short doc
                          "namespace2/uri5",    // 'coffee' 3 times
                          "namespace2/uri1",    // 'food' 2 times
                          "namespace2/uri4",    // 'food' 2 times
                          "namespace2/uri2",    // 'food' 1 time
                          "namespace2/uri6"));  // 'food' 1 time
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultShouldHaveDefaultOrderWithoutUsageTimestamp) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 3 test documents
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // None of the documents have usage reports. Result should be in the default
  // reverse insertion order.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::USAGE_TYPE1_LAST_USED_TIMESTAMP);
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchResultShouldBeRankedAscendingly) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

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
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());

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
  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingDuplicateNamespaceShouldReturnError) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // "m" will match all 2 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Specify "namespace1" twice. This should result in an error.
  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::NAMESPACE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_namespace_("namespace1");
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace2");
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace1");
  result_grouping = result_spec.add_result_groupings();
  entry = result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_namespace_("namespace1");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_result_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingDuplicateSchemaShouldReturnError) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // "m" will match all 2 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Specify "Message" twice. This should result in an error.
  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::SCHEMA_TYPE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_schema("Message");
  entry = result_grouping->add_entry_groupings();
  entry->set_schema("nonexistentMessage");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  entry = result_grouping->add_entry_groupings();
  entry->set_schema("Message");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_result_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingDuplicateNamespaceAndSchemaSchemaShouldReturnError) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // "m" will match all 2 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Specify "namespace1xMessage" twice. This should result in an error.
  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::NAMESPACE_AND_SCHEMA_TYPE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_namespace_("namespace1");
  entry->set_schema("Message");
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace2");
  entry->set_schema("Message");
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace1");
  entry->set_schema("Message");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace1");
  entry->set_schema("Message");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_result_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingNonPositiveMaxResultsShouldReturnError) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // "m" will match all 2 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  // Specify zero results. This should result in an error.
  ResultSpecProto result_spec;
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(0);
  entry->set_namespace_("namespace1");
  entry->set_schema("Message");
  result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace2");
  entry->set_schema("Message");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_result_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));

  // Specify negative results. This should result in an error.
  result_spec.mutable_result_groupings(0)->set_max_results(-1);
  EXPECT_THAT(search_result_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingMultiNamespaceGrouping) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 3 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3 < document4 < document5 <
  // document6
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(3)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document4 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/4")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(4)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document5 =
      DocumentBuilder()
          .SetKey("namespace3", "uri/5")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(5)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document6 =
      DocumentBuilder()
          .SetKey("namespace3", "uri/6")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(6)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document4).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document5).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document6).status(), ProtoIsOk());

  // "m" will match all 6 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::NAMESPACE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_namespace_("namespace1");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(2);
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace2");
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace3");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);

  // The last result (document1) in namespace "namespace1" should not be
  // included. "namespace2" and "namespace3" are grouped together. So only the
  // two highest scored documents between the two (both of which are in
  // "namespace3") should be returned.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document6;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document5;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;

  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SearchResultGroupingMultiSchemaGrouping) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("body")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("sender")
                                        .SetDataTypeDocument(
                                            "Person",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Email")
          .SetScore(1)
          .SetCreationTimestampMs(10)
          .AddStringProperty("subject", "foo")
          .AddDocumentProperty("sender", DocumentBuilder()
                                             .SetKey("namespace", "uri1-sender")
                                             .SetSchema("Person")
                                             .AddStringProperty("name", "foo")
                                             .Build())
          .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace1", "uri2")
                                .SetSchema("Message")
                                .SetScore(2)
                                .SetCreationTimestampMs(10)
                                .AddStringProperty("body", "fo")
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace2", "uri3")
                                .SetSchema("Message")
                                .SetScore(3)
                                .SetCreationTimestampMs(10)
                                .AddStringProperty("body", "fo")
                                .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());

  // "f" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("f");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::SCHEMA_TYPE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_schema("Message");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("Email");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);

  // Each of the highest scored documents of schema type "Message" (document3)
  // and "Email" (document1) should be returned.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingMultiNamespaceAndSchemaGrouping) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 3 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3 < document4 < document5 <
  // document6
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(3)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document4 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/4")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(4)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document5 =
      DocumentBuilder()
          .SetKey("namespace3", "uri/5")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(5)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document6 =
      DocumentBuilder()
          .SetKey("namespace3", "uri/6")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(6)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document4).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document5).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document6).status(), ProtoIsOk());

  // "m" will match all 6 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::NAMESPACE_AND_SCHEMA_TYPE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_namespace_("namespace1");
  entry->set_schema("Message");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace2");
  entry->set_schema("Message");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace3");
  entry->set_schema("Message");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);

  // The three highest scored documents that fit the criteria of
  // "namespace1xMessage" (document2), "namespace2xMessage" (document4),
  // and "namespace3xMessage" (document6) should be returned.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document6;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document4;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;

  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingNonexistentNamespaceShouldBeIgnored) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // "m" will match all 2 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::NAMESPACE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_namespace_("namespace1");
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("nonexistentNamespace");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);

  // Only the top ranked document in "namespace" (document2), should be
  // returned. The presence of "nonexistentNamespace" in the same result
  // grouping should have no effect.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;

  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingNonexistentSchemaShouldBeIgnored) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // "m" will match all 2 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::SCHEMA_TYPE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_schema("Message");
  entry = result_grouping->add_entry_groupings();
  entry->set_schema("nonexistentMessage");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);

  // Only the top ranked document in "Message" (document2), should be
  // returned. The presence of "nonexistentMessage" in the same result
  // grouping should have no effect.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;

  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchResultGroupingNonexistentNamespaceAndSchemaShouldBeIgnored) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace1", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(3)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  DocumentProto document4 =
      DocumentBuilder()
          .SetKey("namespace2", "uri/4")
          .SetSchema("Message")
          .AddStringProperty("body", "message4")
          .SetScore(4)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document4).status(), ProtoIsOk());

  // "m" will match all 2 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);

  ResultSpecProto result_spec;
  result_spec.set_result_group_type(ResultSpecProto::SCHEMA_TYPE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(1);
  entry->set_namespace_("namespace2");
  entry->set_schema("Message");
  entry = result_grouping->add_entry_groupings();
  entry->set_schema("namespace1");
  entry->set_schema("nonexistentMessage");

  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);

  // Only the top ranked document in "namespace2xMessage" (document4), should be
  // returned. The presence of "namespace1xnonexistentMessage" in the same
  // result grouping should have no effect. If either the namespace or the
  // schema type is nonexistent, the entire entry will be ignored.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document4;

  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, SnippetNormalization) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "MDI zurich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "mdi Zürich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("mdi Zürich");

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(2);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(2);

  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  ASSERT_THAT(results.results(), SizeIs(2));
  const DocumentProto& result_document_1 = results.results(0).document();
  const SnippetProto& result_snippet_1 = results.results(0).snippet();
  EXPECT_THAT(result_document_1, EqualsProto(document_two));
  EXPECT_THAT(result_snippet_1.entries(), SizeIs(1));
  EXPECT_THAT(result_snippet_1.entries(0).property_name(), Eq("body"));
  std::string_view content = GetString(
      &result_document_1, result_snippet_1.entries(0).property_name());
  EXPECT_THAT(
      GetWindows(content, result_snippet_1.entries(0)),
      ElementsAre("mdi Zürich Team Meeting", "mdi Zürich Team Meeting"));
  EXPECT_THAT(GetMatches(content, result_snippet_1.entries(0)),
              ElementsAre("mdi", "Zürich"));

  const DocumentProto& result_document_2 = results.results(1).document();
  const SnippetProto& result_snippet_2 = results.results(1).snippet();
  EXPECT_THAT(result_document_2, EqualsProto(document_one));
  EXPECT_THAT(result_snippet_2.entries(), SizeIs(1));
  EXPECT_THAT(result_snippet_2.entries(0).property_name(), Eq("body"));
  content = GetString(&result_document_2,
                      result_snippet_2.entries(0).property_name());
  EXPECT_THAT(
      GetWindows(content, result_snippet_2.entries(0)),
      ElementsAre("MDI zurich Team Meeting", "MDI zurich Team Meeting"));
  EXPECT_THAT(GetMatches(content, result_snippet_2.entries(0)),
              ElementsAre("MDI", "zurich"));
}

TEST_F(IcingSearchEngineSearchTest, SnippetNormalizationPrefix) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "MDI zurich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "mdi Zürich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("md Zür");

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(2);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(2);

  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  ASSERT_THAT(results.results(), SizeIs(2));
  const DocumentProto& result_document_1 = results.results(0).document();
  const SnippetProto& result_snippet_1 = results.results(0).snippet();
  EXPECT_THAT(result_document_1, EqualsProto(document_two));
  EXPECT_THAT(result_snippet_1.entries(), SizeIs(1));
  EXPECT_THAT(result_snippet_1.entries(0).property_name(), Eq("body"));
  std::string_view content = GetString(
      &result_document_1, result_snippet_1.entries(0).property_name());
  EXPECT_THAT(
      GetWindows(content, result_snippet_1.entries(0)),
      ElementsAre("mdi Zürich Team Meeting", "mdi Zürich Team Meeting"));
  EXPECT_THAT(GetMatches(content, result_snippet_1.entries(0)),
              ElementsAre("mdi", "Zürich"));

  const DocumentProto& result_document_2 = results.results(1).document();
  const SnippetProto& result_snippet_2 = results.results(1).snippet();
  EXPECT_THAT(result_document_2, EqualsProto(document_one));
  EXPECT_THAT(result_snippet_2.entries(), SizeIs(1));
  EXPECT_THAT(result_snippet_2.entries(0).property_name(), Eq("body"));
  content = GetString(&result_document_2,
                      result_snippet_2.entries(0).property_name());
  EXPECT_THAT(
      GetWindows(content, result_snippet_2.entries(0)),
      ElementsAre("MDI zurich Team Meeting", "MDI zurich Team Meeting"));
  EXPECT_THAT(GetMatches(content, result_snippet_2.entries(0)),
              ElementsAre("MDI", "zurich"));
}

TEST_F(IcingSearchEngineSearchTest, SnippetSectionRestrict) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Email")
          .AddStringProperty("subject", "MDI zurich Team Meeting")
          .AddStringProperty("body", "MDI zurich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Email")
          .AddStringProperty("subject", "MDI zurich trip")
          .AddStringProperty("body", "Let's travel to zurich")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("body:Zür");

  auto result_spec = std::make_unique<ResultSpecProto>();
  result_spec->set_num_per_page(1);
  result_spec->mutable_snippet_spec()->set_max_window_utf32_length(64);
  result_spec->mutable_snippet_spec()->set_num_matches_per_property(10);
  result_spec->mutable_snippet_spec()->set_num_to_snippet(10);

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();

  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  ASSERT_THAT(results.results(), SizeIs(1));

  const DocumentProto& result_document_two = results.results(0).document();
  const SnippetProto& result_snippet_two = results.results(0).snippet();
  EXPECT_THAT(result_document_two, EqualsProto(document_two));
  EXPECT_THAT(result_snippet_two.entries(), SizeIs(1));
  EXPECT_THAT(result_snippet_two.entries(0).property_name(), Eq("body"));
  std::string_view content = GetString(
      &result_document_two, result_snippet_two.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet_two.entries(0)),
              ElementsAre("Let's travel to zurich"));
  EXPECT_THAT(GetMatches(content, result_snippet_two.entries(0)),
              ElementsAre("zurich"));

  search_spec.reset();
  scoring_spec.reset();
  result_spec.reset();

  results = icing.GetNextPage(results.next_page_token());
  EXPECT_THAT(results.status(), ProtoIsOk());
  ASSERT_THAT(results.results(), SizeIs(1));

  const DocumentProto& result_document_one = results.results(0).document();
  const SnippetProto& result_snippet_one = results.results(0).snippet();
  EXPECT_THAT(result_document_one, EqualsProto(document_one));
  EXPECT_THAT(result_snippet_one.entries(), SizeIs(1));
  EXPECT_THAT(result_snippet_one.entries(0).property_name(), Eq("body"));
  content = GetString(&result_document_one,
                      result_snippet_one.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, result_snippet_one.entries(0)),
              ElementsAre("MDI zurich Team Meeting"));
  EXPECT_THAT(GetMatches(content, result_snippet_one.entries(0)),
              ElementsAre("zurich"));
}

TEST_F(IcingSearchEngineSearchTest, Hyphens) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SchemaProto schema;
  SchemaTypeConfigProto* type = schema.add_types();
  type->set_schema_type("MyType");
  PropertyConfigProto* prop = type->add_properties();
  prop->set_property_name("foo");
  prop->set_data_type(PropertyConfigProto::DataType::STRING);
  prop->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  prop->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  prop->mutable_string_indexing_config()->set_tokenizer_type(
      StringIndexingConfig::TokenizerType::PLAIN);
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("MyType")
          .AddStringProperty("foo", "foo bar-baz bat")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("MyType")
          .AddStringProperty("foo", "bar for baz bat-man")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("foo:bar-baz");

  ResultSpecProto result_spec;
  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);

  EXPECT_THAT(results.status(), ProtoIsOk());
  ASSERT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithProjectionEmptyFieldPath) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "shopgirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query that will match those documents and use an empty field
  // mask to request NO properties.
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("hello");

  ResultSpecProto result_spec;
  // Retrieve only one result at a time to make sure that projection works when
  // retrieving all pages.
  result_spec.set_num_per_page(1);
  TypePropertyMask* email_field_mask = result_spec.add_type_property_masks();
  email_field_mask->set_schema_type("Email");
  email_field_mask->add_paths("");

  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));

  // 3. Verify that the returned results contain no properties.
  DocumentProto projected_document_two = DocumentBuilder()
                                             .SetKey("namespace", "uri2")
                                             .SetCreationTimestampMs(1000)
                                             .SetSchema("Email")
                                             .Build();
  EXPECT_THAT(results.results(0).document(),
              EqualsProto(projected_document_two));

  results = icing.GetNextPage(results.next_page_token());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  DocumentProto projected_document_one = DocumentBuilder()
                                             .SetKey("namespace", "uri1")
                                             .SetCreationTimestampMs(1000)
                                             .SetSchema("Email")
                                             .Build();
  EXPECT_THAT(results.results(0).document(),
              EqualsProto(projected_document_one));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithProjectionMultipleFieldPaths) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "shopgirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query that will match those documents and request only
  // 'sender.name' and 'subject' properties.
  // Create all of search_spec, result_spec and scoring_spec as objects with
  // scope that will end before the call to GetNextPage to ensure that the
  // implementation isn't relying on references to any of them.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("hello");

  auto result_spec = std::make_unique<ResultSpecProto>();
  // Retrieve only one result at a time to make sure that projection works when
  // retrieving all pages.
  result_spec->set_num_per_page(1);
  TypePropertyMask* email_field_mask = result_spec->add_type_property_masks();
  email_field_mask->set_schema_type("Email");
  email_field_mask->add_paths("sender.name");
  email_field_mask->add_paths("subject");

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));

  // 3. Verify that the first returned result only contains the 'sender.name'
  // property.
  DocumentProto projected_document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty("sender",
                               DocumentBuilder()
                                   .SetKey("namespace", "uri2")
                                   .SetSchema("Person")
                                   .AddStringProperty("name", "Tom Hanks")
                                   .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .Build();
  EXPECT_THAT(results.results(0).document(),
              EqualsProto(projected_document_two));

  // 4. Now, delete all of the specs used in the search. GetNextPage should have
  // no problem because it shouldn't be keeping any references to them.
  search_spec.reset();
  result_spec.reset();
  scoring_spec.reset();

  // 5. Verify that the second returned result only contains the 'sender.name'
  // property.
  results = icing.GetNextPage(results.next_page_token());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  DocumentProto projected_document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty("sender",
                               DocumentBuilder()
                                   .SetKey("namespace", "uri1")
                                   .SetSchema("Person")
                                   .AddStringProperty("name", "Meg Ryan")
                                   .Build())
          .AddStringProperty("subject", "Hello World!")
          .Build();
  EXPECT_THAT(results.results(0).document(),
              EqualsProto(projected_document_one));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithPolymorphicProjectionAndExactSchemaFilter) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Artist")
                       .AddParentType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("company")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // Add a person document and an artist document
  DocumentProto document_person =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Person")
          .AddStringProperty("name", "Foo Person")
          .AddStringProperty("emailAddress", "person@gmail.com")
          .Build();
  DocumentProto document_artist =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Artist")
          .AddStringProperty("name", "Foo Artist")
          .AddStringProperty("emailAddress", "artist@gmail.com")
          .AddStringProperty("company", "Company")
          .Build();
  ASSERT_THAT(icing.Put(document_person).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document_artist).status(), ProtoIsOk());

  // Issue a query with a exact schema filter for "Person", which will **not**
  // be expanded to "Artist" via polymorphism, and test that projection works
  // for both types even though artist will not be returned at all.
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("Foo");
  search_spec.add_schema_type_filters("Person");

  ResultSpecProto result_spec;
  TypePropertyMask* person_field_mask = result_spec.add_type_property_masks();
  person_field_mask->set_schema_type("Person");
  person_field_mask->add_paths("name");
  TypePropertyMask* artist_field_mask = result_spec.add_type_property_masks();
  artist_field_mask->set_schema_type("Artist");
  artist_field_mask->add_paths("emailAddress");

  // Verify results
  DocumentProto projected_document_person =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Person")
          .AddStringProperty("name", "Foo Person")
          .Build();
  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(),
              EqualsProto(projected_document_person));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithPropertyFilters) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "hellogirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query with property filters of sender.name and subject for the
  // Email schema type.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("hello");

  TypePropertyMask* email_property_filters =
      search_spec->add_type_property_filters();
  email_property_filters->set_schema_type("Email");
  email_property_filters->add_paths("sender.name");
  email_property_filters->add_paths("subject");

  auto result_spec = std::make_unique<ResultSpecProto>();

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));

  // 3. Verify that only the first document is returned. Although 'hello' is
  // present in document_two, it shouldn't be in the result since 'hello' is not
  // in the specified property filter.
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_one));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithPropertyFiltersPolymorphism) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Artist")
                       .AddParentType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("company")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // Add a person document and an artist document
  DocumentProto document_person =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Person")
          .AddStringProperty("name", "Meg Ryan")
          .AddStringProperty("emailAddress", "shopgirl@aol.com")
          .Build();
  DocumentProto document_artist =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Artist")
          .AddStringProperty("name", "Meg Artist")
          .AddStringProperty("emailAddress", "artist@aol.com")
          .AddStringProperty("company", "company")
          .Build();
  ASSERT_THAT(icing.Put(document_person).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document_artist).status(), ProtoIsOk());

  // Set a query with property filters of "name" in Person and "emailAddress"
  // in Artist. By polymorphism, "name" should also apply to Artist.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);

  TypePropertyMask* person_type_property_mask =
      search_spec->add_type_property_filters();
  person_type_property_mask->set_schema_type("Person");
  person_type_property_mask->add_paths("name");
  TypePropertyMask* artist_type_property_mask =
      search_spec->add_type_property_filters();
  artist_type_property_mask->set_schema_type("Artist");
  artist_type_property_mask->add_paths("emailAddress");

  auto result_spec = std::make_unique<ResultSpecProto>();
  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();

  // Verify that the property filter for "name" in Person is also applied to
  // Artist.
  search_spec->set_query("Meg");
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_person));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_artist));

  // Verify that the property filter for "emailAddress" in Artist is only
  // applied to Artist.
  search_spec->set_query("aol");
  results = icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_artist));

  // Verify that the "company" property is filtered out, since it is not
  // specified in the property filter.
  search_spec->set_query("company");
  results = icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, EmptySearchWithPropertyFilter) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "hellogirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query with a property filter
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("");

  TypePropertyMask* email_property_filters =
      search_spec->add_type_property_filters();
  email_property_filters->set_schema_type("Email");
  email_property_filters->add_paths("subject");

  auto result_spec = std::make_unique<ResultSpecProto>();

  // 3. Verify that both documents are returned.
  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
}

TEST_F(IcingSearchEngineSearchTest, EmptySearchWithEmptyPropertyFilter) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "hellogirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query with a property filter
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("");

  TypePropertyMask* email_property_filters =
      search_spec->add_type_property_filters();
  // Add empty list for Email's property filters
  email_property_filters->set_schema_type("Email");

  auto result_spec = std::make_unique<ResultSpecProto>();

  // 3. Verify that both documents are returned.
  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithPropertyFiltersOnMultipleSchema) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  // Add Person and Organization schema with a property 'name' in both.
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Organization")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("address")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // 1. Add person document
  DocumentProto person_document =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Person")
          .AddStringProperty("name", "Meg Ryan")
          .AddStringProperty("emailAddress", "hellogirl@aol.com")
          .Build();
  ASSERT_THAT(icing.Put(person_document).status(), ProtoIsOk());

  // 1. Add organization document
  DocumentProto organization_document =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Organization")
          .AddStringProperty("name", "Meg Corp")
          .AddStringProperty("address", "Universal street")
          .Build();
  ASSERT_THAT(icing.Put(organization_document).status(), ProtoIsOk());

  // 2. Issue a query with property filters. Person schema has name in it's
  // property filter but Organization schema doesn't.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("Meg");

  TypePropertyMask* person_property_filters =
      search_spec->add_type_property_filters();
  person_property_filters->set_schema_type("Person");
  person_property_filters->add_paths("name");
  TypePropertyMask* organization_property_filters =
      search_spec->add_type_property_filters();
  organization_property_filters->set_schema_type("Organization");
  organization_property_filters->add_paths("address");

  auto result_spec = std::make_unique<ResultSpecProto>();

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));

  // 3. Verify that only the person document is returned. Although 'Meg' is
  // present in organization document, it shouldn't be in the result since
  // the name field is not specified in the Organization property filter.
  EXPECT_THAT(results.results(0).document(), EqualsProto(person_document));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithWildcardPropertyFilters) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "hellogirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query with property filters of sender.name and subject for the
  // wildcard(*) schema type.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("hello");

  TypePropertyMask* wildcard_property_filters =
      search_spec->add_type_property_filters();
  wildcard_property_filters->set_schema_type("*");
  wildcard_property_filters->add_paths("sender.name");
  wildcard_property_filters->add_paths("subject");

  auto result_spec = std::make_unique<ResultSpecProto>();

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));

  // 3. Verify that only the first document is returned since the second
  // document doesn't contain the word 'hello' in either of fields specified in
  // the property filter. This confirms that the property filters for the
  // wildcard entry have been applied to the Email schema as well.
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_one));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithMixedPropertyFilters) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "hellogirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query with property filters of sender.name and subject for the
  // wildcard(*) schema type plus property filters of sender.name and body for
  // the Email schema type.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("hello");

  TypePropertyMask* wildcard_property_filters =
      search_spec->add_type_property_filters();
  wildcard_property_filters->set_schema_type("*");
  wildcard_property_filters->add_paths("sender.name");
  wildcard_property_filters->add_paths("subject");
  TypePropertyMask* email_property_filters =
      search_spec->add_type_property_filters();
  email_property_filters->set_schema_type("Email");
  email_property_filters->add_paths("sender.name");
  email_property_filters->add_paths("body");

  auto result_spec = std::make_unique<ResultSpecProto>();

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));

  // 3. Verify that only the second document is returned since the first
  // document doesn't contain the word 'hello' in either of fields sender.name
  // or body. This confirms that the property filters specified for Email schema
  // have been applied and the ones specified for wildcard entry have been
  // ignored.
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithNonApplicablePropertyFilters) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "hellogirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query with property filters of sender.name and subject for an
  // unknown schema type.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("hello");

  TypePropertyMask* email_property_filters =
      search_spec->add_type_property_filters();
  email_property_filters->set_schema_type("unknown");
  email_property_filters->add_paths("sender.name");
  email_property_filters->add_paths("subject");

  auto result_spec = std::make_unique<ResultSpecProto>();

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));

  // 3. Verify that both the documents are returned since each of them have the
  // word 'hello' in at least 1 property. The second document being returned
  // confirms that the body field was searched and the specified property
  // filters were not applied to the Email schema type.
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithEmptyPropertyFilter) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one = DocumentBuilder()
                                   .SetKey("namespace", "uri1")
                                   .SetCreationTimestampMs(1000)
                                   .SetSchema("Message")
                                   .AddStringProperty("body", "Hello World!")
                                   .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  // 2. Issue a query with empty property filter for Message schema.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("hello");

  TypePropertyMask* message_property_filters =
      search_spec->add_type_property_filters();
  message_property_filters->set_schema_type("Message");

  auto result_spec = std::make_unique<ResultSpecProto>();

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());

  // 3. Verify that no documents are returned. Although 'hello' is present in
  // the indexed document, it shouldn't be returned since the Message property
  // filter doesn't allow any properties to be searched.
  ASSERT_THAT(results.results(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithPropertyFilterHavingInvalidProperty) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one = DocumentBuilder()
                                   .SetKey("namespace", "uri1")
                                   .SetCreationTimestampMs(1000)
                                   .SetSchema("Message")
                                   .AddStringProperty("body", "Hello World!")
                                   .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  // 2. Issue a query with property filter having invalid/unknown property for
  // Message schema.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("hello");

  TypePropertyMask* message_property_filters =
      search_spec->add_type_property_filters();
  message_property_filters->set_schema_type("Message");
  message_property_filters->add_paths("unknown");

  auto result_spec = std::make_unique<ResultSpecProto>();

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());

  // 3. Verify that no documents are returned. Although 'hello' is present in
  // the indexed document, it shouldn't be returned since the Message property
  // filter doesn't allow any valid properties to be searched. Any
  // invalid/unknown properties specified in the property filters will be
  // ignored while searching.
  ASSERT_THAT(results.results(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, SearchWithPropertyFiltersWithNesting) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("Person")
                  .AddStringProperty("name", "Meg Ryan")
                  .AddStringProperty("emailAddress", "hellogirl@aol.com")
                  .Build())
          .AddStringProperty("subject", "Hello World!")
          .AddStringProperty(
              "body", "Oh what a beautiful morning! Oh what a beautiful day!")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "ny152@aol.com")
                            .Build())
          .AddStringProperty("subject", "Goodnight Moon!")
          .AddStringProperty("body",
                             "Count all the sheep and tell them 'Hello'.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query with property filter of sender.emailAddress for the Email
  // schema type.
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("hello");

  TypePropertyMask* email_property_filters =
      search_spec->add_type_property_filters();
  email_property_filters->set_schema_type("Email");
  email_property_filters->add_paths("sender.emailAddress");

  auto result_spec = std::make_unique<ResultSpecProto>();

  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  *scoring_spec = GetDefaultScoringSpec();
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));

  // 3. Verify that only the first document is returned since the second
  // document doesn't contain the word 'hello' in sender.emailAddress. The first
  // document being returned confirms that the nested property
  // sender.emailAddress was actually searched.
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_one));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithPropertyFilter_RelevanceScoreUnaffectedByExcludedSectionHits) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add two email documents
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri1")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Hello Ryan")
                            .AddStringProperty("emailAddress", "hello@aol.com")
                            .Build())
          .AddStringProperty("subject", "Hello Hello!")
          .AddStringProperty("body", "hello1 hello2 hello3 hello4 hello5")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri2")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Tom Hanks")
                            .AddStringProperty("emailAddress", "world@aol.com")
                            .Build())
          .AddStringProperty("subject", "Hello Hello!")
          .AddStringProperty("body", "one1 two2 three3 four4 five5")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // 2. Issue a query with a property filter
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("Hello");

  TypePropertyMask* email_property_filters =
      search_spec->add_type_property_filters();
  email_property_filters->set_schema_type("Email");
  email_property_filters->add_paths("subject");

  auto result_spec = std::make_unique<ResultSpecProto>();

  // 3. Verify that both documents are returned and have equal relevance score
  // Note, the total number of tokens must be equal in the documents
  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  scoring_spec->set_rank_by(ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE);
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  ASSERT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).score(), DoubleEq(results.results(1).score()));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithPropertyFilter_ExcludingSectionsWithHitsLowersRelevanceScore) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreatePersonAndEmailSchema()).status(),
              ProtoIsOk());

  // 1. Add an email document
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetCreationTimestampMs(1000)
          .SetSchema("Email")
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri1")
                            .SetSchema("Person")
                            .AddStringProperty("name", "Hello Ryan")
                            .AddStringProperty("emailAddress", "hello@aol.com")
                            .Build())
          .AddStringProperty("subject", "Hello Hello!")
          .AddStringProperty("body", "hello hello hello hello hello")
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  // 2. Issue a query without property filter
  auto search_spec = std::make_unique<SearchSpecProto>();
  search_spec->set_term_match_type(TermMatchType::PREFIX);
  search_spec->set_query("Hello");

  auto result_spec = std::make_unique<ResultSpecProto>();

  // 3. Get the relevance score without property filter
  auto scoring_spec = std::make_unique<ScoringSpecProto>();
  scoring_spec->set_rank_by(ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE);
  SearchResultProto results =
      icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  ASSERT_THAT(results.results(), SizeIs(1));
  double original_relevance_score = results.results(0).score();

  // 4. Relevance score with property filter should be lower
  TypePropertyMask* email_property_filters =
      search_spec->add_type_property_filters();
  email_property_filters->set_schema_type("Email");
  email_property_filters->add_paths("subject");
  results = icing.Search(*search_spec, *scoring_spec, *result_spec);
  EXPECT_THAT(results.status(), ProtoIsOk());
  ASSERT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).score(), Lt(original_relevance_score));
}

TEST_F(IcingSearchEngineSearchTest, QueryStatsProtoTest) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(5);

  // Set index merge size to 6 hits. This will cause document1, document2,
  // document3's hits being merged into the main index, and document4,
  // document5's hits will remain in the lite index.
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(sizeof(TermIdHitPair::Value) * 6);

  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Creates and inserts 5 documents
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");
  DocumentProto document4 = CreateMessageDocument("namespace", "uri4");
  DocumentProto document5 = CreateMessageDocument("namespace", "uri5");
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document4).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document5).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.add_namespace_filters("namespace");
  search_spec.add_schema_type_filters(document1.schema());
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(2);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(1);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(3);

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);

  // Searches and gets the first page, 2 results with 2 snippets
  SearchResultProto search_result =
      icing.Search(search_spec, scoring_spec, result_spec);
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  ASSERT_THAT(search_result.results(), SizeIs(2));
  ASSERT_THAT(search_result.next_page_token(), Ne(kInvalidNextPageToken));

  // Check the stats
  // TODO(b/305098009): deprecate search-related flat fields in query_stats.
  QueryStatsProto exp_stats;
  exp_stats.set_query_length(7);
  exp_stats.set_num_terms(1);
  exp_stats.set_num_namespaces_filtered(1);
  exp_stats.set_num_schema_types_filtered(1);
  exp_stats.set_ranking_strategy(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);
  exp_stats.set_is_first_page(true);
  exp_stats.set_requested_page_size(2);
  exp_stats.set_num_results_returned_current_page(2);
  exp_stats.set_num_documents_scored(5);
  exp_stats.set_num_results_with_snippets(2);
  exp_stats.set_latency_ms(5);
  exp_stats.set_parse_query_latency_ms(5);
  exp_stats.set_scoring_latency_ms(5);
  exp_stats.set_ranking_latency_ms(5);
  exp_stats.set_document_retrieval_latency_ms(5);
  exp_stats.set_lock_acquisition_latency_ms(5);
  exp_stats.set_num_joined_results_returned_current_page(0);
  // document4, document5's hits will remain in the lite index (# of hits: 4).
  exp_stats.set_lite_index_hit_buffer_byte_size(4 *
                                                sizeof(TermIdHitPair::Value));
  exp_stats.set_lite_index_hit_buffer_unsorted_byte_size(
      4 * sizeof(TermIdHitPair::Value));

  QueryStatsProto::SearchStats* exp_parent_search_stats =
      exp_stats.mutable_parent_search_stats();
  exp_parent_search_stats->set_query_length(7);
  exp_parent_search_stats->set_num_terms(1);
  exp_parent_search_stats->set_num_namespaces_filtered(1);
  exp_parent_search_stats->set_num_schema_types_filtered(1);
  exp_parent_search_stats->set_ranking_strategy(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);
  exp_parent_search_stats->set_num_documents_scored(5);
  exp_parent_search_stats->set_parse_query_latency_ms(5);
  exp_parent_search_stats->set_scoring_latency_ms(5);
  exp_parent_search_stats->set_num_fetched_hits_lite_index(2);
  exp_parent_search_stats->set_num_fetched_hits_main_index(3);
  exp_parent_search_stats->set_num_fetched_hits_integer_index(0);
  exp_parent_search_stats->set_query_processor_lexer_extract_token_latency_ms(
      5);
  exp_parent_search_stats->set_query_processor_parser_consume_query_latency_ms(
      5);
  exp_parent_search_stats->set_query_processor_query_visitor_latency_ms(5);

  EXPECT_THAT(search_result.query_stats(), EqualsProto(exp_stats));

  // Second page, 2 result with 1 snippet
  search_result = icing.GetNextPage(search_result.next_page_token());
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  ASSERT_THAT(search_result.results(), SizeIs(2));
  ASSERT_THAT(search_result.next_page_token(), Gt(kInvalidNextPageToken));

  exp_stats = QueryStatsProto();
  exp_stats.set_is_first_page(false);
  exp_stats.set_requested_page_size(2);
  exp_stats.set_num_results_returned_current_page(2);
  exp_stats.set_num_results_with_snippets(1);
  exp_stats.set_latency_ms(5);
  exp_stats.set_document_retrieval_latency_ms(5);
  exp_stats.set_lock_acquisition_latency_ms(5);
  exp_stats.set_num_joined_results_returned_current_page(0);
  EXPECT_THAT(search_result.query_stats(), EqualsProto(exp_stats));

  // Third page, 1 result with 0 snippets
  search_result = icing.GetNextPage(search_result.next_page_token());
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  ASSERT_THAT(search_result.results(), SizeIs(1));
  ASSERT_THAT(search_result.next_page_token(), Eq(kInvalidNextPageToken));

  exp_stats = QueryStatsProto();
  exp_stats.set_is_first_page(false);
  exp_stats.set_requested_page_size(2);
  exp_stats.set_num_results_returned_current_page(1);
  exp_stats.set_num_results_with_snippets(0);
  exp_stats.set_latency_ms(5);
  exp_stats.set_document_retrieval_latency_ms(5);
  exp_stats.set_lock_acquisition_latency_ms(5);
  exp_stats.set_num_joined_results_returned_current_page(0);
  EXPECT_THAT(search_result.query_stats(), EqualsProto(exp_stats));
}

TEST_F(IcingSearchEngineSearchTest, JoinQueryStatsProtoTest) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(5);

  // Set index merge size to 13 hits. This will cause person1, person2, email1,
  // email2, email3's hits being merged into the main index, and person3,
  // email4's hits will remain in the lite index.
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(sizeof(TermIdHitPair::Value) * 13);

  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("personQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person1")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first1")
          .AddStringProperty("lastName", "last1")
          .AddStringProperty("emailAddress", "email1@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person2")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first2")
          .AddStringProperty("lastName", "last2")
          .AddStringProperty("emailAddress", "email2@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(2)
          .Build();
  DocumentProto person3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person3")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first3")
          .AddStringProperty("lastName", "last3")
          .AddStringProperty("emailAddress", "email3@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(3)
          .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(3)
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(2)
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();
  DocumentProto email4 =
      DocumentBuilder()
          .SetKey("namespace", "email4")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 4")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(0)
          .Build();

  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email4).status(), ProtoIsOk());

  // Parent SearchSpec
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("firstName:first");

  // JoinSpec
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::PREFIX);
  nested_search_spec->set_query("subject:test");

  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  // Parent ScoringSpec
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::JOIN_AGGREGATE_SCORE);
  scoring_spec.set_order_by(ScoringSpecProto::Order::DESC);

  // Parent ResultSpec
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  // Since we:
  // - Use COUNT for aggregation scoring strategy.
  // - (Default) use DOCUMENT_SCORE to score child documents.
  // - (Default) use DESC as the ranking order.
  //
  // person1 with [email1, email2, email4] should have the highest aggregated
  // score (3) and be returned first. person2 with [email3] (aggregated score =
  // 1) should be the second, and person3 with no child (aggregated score = 0)
  // should be the last.
  SearchResultProto expected_result1;
  expected_result1.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_proto1 =
      expected_result1.mutable_results()->Add();
  *result_proto1->mutable_document() = person1;
  *result_proto1->mutable_joined_results()->Add()->mutable_document() = email1;
  *result_proto1->mutable_joined_results()->Add()->mutable_document() = email2;
  *result_proto1->mutable_joined_results()->Add()->mutable_document() = email4;

  SearchResultProto expected_result2;
  expected_result2.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_google::protobuf =
      expected_result2.mutable_results()->Add();
  *result_google::protobuf->mutable_document() = person2;
  *result_google::protobuf->mutable_joined_results()->Add()->mutable_document() = email3;

  SearchResultProto expected_result3;
  expected_result3.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_proto3 =
      expected_result3.mutable_results()->Add();
  *result_proto3->mutable_document() = person3;

  SearchResultProto search_result =
      icing.Search(search_spec, scoring_spec, result_spec);
  uint64_t next_page_token = search_result.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  expected_result1.set_next_page_token(next_page_token);
  ASSERT_THAT(search_result,
              EqualsSearchResultIgnoreStatsAndScores(expected_result1));

  // Check the stats
  // TODO(b/305098009): deprecate search-related flat fields in query_stats.
  QueryStatsProto exp_stats;
  exp_stats.set_query_length(15);
  exp_stats.set_num_terms(1);
  exp_stats.set_num_namespaces_filtered(0);
  exp_stats.set_num_schema_types_filtered(0);
  exp_stats.set_ranking_strategy(
      ScoringSpecProto::RankingStrategy::JOIN_AGGREGATE_SCORE);
  exp_stats.set_is_first_page(true);
  exp_stats.set_requested_page_size(1);
  exp_stats.set_num_results_returned_current_page(1);
  exp_stats.set_num_documents_scored(3);
  exp_stats.set_num_results_with_snippets(0);
  exp_stats.set_latency_ms(5);
  exp_stats.set_parse_query_latency_ms(5);
  exp_stats.set_scoring_latency_ms(5);
  exp_stats.set_ranking_latency_ms(5);
  exp_stats.set_document_retrieval_latency_ms(5);
  exp_stats.set_lock_acquisition_latency_ms(5);
  exp_stats.set_num_joined_results_returned_current_page(3);
  exp_stats.set_join_latency_ms(5);
  exp_stats.set_is_join_query(true);
  // person3, email4's hits will remain in the lite index (# of hits: 5).
  exp_stats.set_lite_index_hit_buffer_byte_size(5 *
                                                sizeof(TermIdHitPair::Value));
  exp_stats.set_lite_index_hit_buffer_unsorted_byte_size(
      5 * sizeof(TermIdHitPair::Value));

  QueryStatsProto::SearchStats* exp_parent_search_stats =
      exp_stats.mutable_parent_search_stats();
  exp_parent_search_stats->set_query_length(15);
  exp_parent_search_stats->set_num_terms(1);
  exp_parent_search_stats->set_num_namespaces_filtered(0);
  exp_parent_search_stats->set_num_schema_types_filtered(0);
  exp_parent_search_stats->set_ranking_strategy(
      ScoringSpecProto::RankingStrategy::JOIN_AGGREGATE_SCORE);
  exp_parent_search_stats->set_num_documents_scored(3);
  exp_parent_search_stats->set_parse_query_latency_ms(5);
  exp_parent_search_stats->set_scoring_latency_ms(5);
  exp_parent_search_stats->set_num_fetched_hits_lite_index(1);
  exp_parent_search_stats->set_num_fetched_hits_main_index(2);
  exp_parent_search_stats->set_num_fetched_hits_integer_index(0);
  exp_parent_search_stats->set_query_processor_lexer_extract_token_latency_ms(
      5);
  exp_parent_search_stats->set_query_processor_parser_consume_query_latency_ms(
      5);
  exp_parent_search_stats->set_query_processor_query_visitor_latency_ms(5);

  QueryStatsProto::SearchStats* exp_child_search_stats =
      exp_stats.mutable_child_search_stats();
  exp_child_search_stats->set_query_length(12);
  exp_child_search_stats->set_num_terms(1);
  exp_child_search_stats->set_num_namespaces_filtered(0);
  exp_child_search_stats->set_num_schema_types_filtered(0);
  exp_child_search_stats->set_ranking_strategy(
      ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  exp_child_search_stats->set_num_documents_scored(4);
  exp_child_search_stats->set_parse_query_latency_ms(5);
  exp_child_search_stats->set_scoring_latency_ms(5);
  exp_child_search_stats->set_num_fetched_hits_lite_index(1);
  exp_child_search_stats->set_num_fetched_hits_main_index(3);
  exp_child_search_stats->set_num_fetched_hits_integer_index(0);
  exp_child_search_stats->set_query_processor_lexer_extract_token_latency_ms(5);
  exp_child_search_stats->set_query_processor_parser_consume_query_latency_ms(
      5);
  exp_child_search_stats->set_query_processor_query_visitor_latency_ms(5);

  EXPECT_THAT(search_result.query_stats(), EqualsProto(exp_stats));

  // Second page, 1 child doc.
  search_result = icing.GetNextPage(next_page_token);
  next_page_token = search_result.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  expected_result2.set_next_page_token(next_page_token);
  EXPECT_THAT(search_result,
              EqualsSearchResultIgnoreStatsAndScores(expected_result2));

  exp_stats = QueryStatsProto();
  exp_stats.set_is_first_page(false);
  exp_stats.set_requested_page_size(1);
  exp_stats.set_num_results_returned_current_page(1);
  exp_stats.set_num_results_with_snippets(0);
  exp_stats.set_latency_ms(5);
  exp_stats.set_document_retrieval_latency_ms(5);
  exp_stats.set_lock_acquisition_latency_ms(5);
  exp_stats.set_num_joined_results_returned_current_page(1);
  EXPECT_THAT(search_result.query_stats(), EqualsProto(exp_stats));

  // Third page, 0 child docs.
  search_result = icing.GetNextPage(next_page_token);
  next_page_token = search_result.next_page_token();
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  ASSERT_THAT(search_result.results(), SizeIs(1));
  ASSERT_THAT(search_result.next_page_token(), Eq(kInvalidNextPageToken));

  exp_stats = QueryStatsProto();
  exp_stats.set_is_first_page(false);
  exp_stats.set_requested_page_size(1);
  exp_stats.set_num_results_returned_current_page(1);
  exp_stats.set_num_joined_results_returned_current_page(0);
  exp_stats.set_latency_ms(5);
  exp_stats.set_document_retrieval_latency_ms(5);
  exp_stats.set_lock_acquisition_latency_ms(5);
  exp_stats.set_num_results_with_snippets(0);
  ASSERT_THAT(search_result,
              EqualsSearchResultIgnoreStatsAndScores(expected_result3));
  EXPECT_THAT(search_result.query_stats(), EqualsProto(exp_stats));

  ASSERT_THAT(search_result.next_page_token(), Eq(kInvalidNextPageToken));

  search_result = icing.GetNextPage(search_result.next_page_token());
  ASSERT_THAT(search_result.status(), ProtoIsOk());
  ASSERT_THAT(search_result.results(), IsEmpty());
  ASSERT_THAT(search_result.next_page_token(), Eq(kInvalidNextPageToken));

  exp_stats = QueryStatsProto();
  exp_stats.set_is_first_page(false);
  exp_stats.set_lock_acquisition_latency_ms(5);
  EXPECT_THAT(search_result.query_stats(), EqualsProto(exp_stats));
}

TEST_F(IcingSearchEngineSearchTest, SnippetErrorTest) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Generic").AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetScore(10)
          .SetSchema("Generic")
          .AddStringProperty("subject", "I like cats", "I like dogs",
                             "I like birds", "I like fish")
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetScore(20)
          .SetSchema("Generic")
          .AddStringProperty("subject", "I like red", "I like green",
                             "I like blue", "I like yellow")
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri3")
          .SetScore(5)
          .SetSchema("Generic")
          .AddStringProperty("subject", "I like cupcakes", "I like donuts",
                             "I like eclairs", "I like froyo")
          .Build();
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters("Generic");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("like");

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_num_to_snippet(2);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(3);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(4);
  SearchResultProto search_results =
      icing.Search(search_spec, scoring_spec, result_spec);

  ASSERT_THAT(search_results.results(), SizeIs(3));
  const SearchResultProto::ResultProto* result = &search_results.results(0);
  EXPECT_THAT(result->document().uri(), Eq("uri2"));
  ASSERT_THAT(result->snippet().entries(), SizeIs(3));
  const SnippetProto::EntryProto* entry = &result->snippet().entries(0);
  EXPECT_THAT(entry->property_name(), "subject[0]");
  std::string_view content = GetString(&result->document(), "subject[0]");
  EXPECT_THAT(GetMatches(content, *entry), ElementsAre("like"));

  entry = &result->snippet().entries(1);
  EXPECT_THAT(entry->property_name(), "subject[1]");
  content = GetString(&result->document(), "subject[1]");
  EXPECT_THAT(GetMatches(content, *entry), ElementsAre("like"));

  entry = &result->snippet().entries(2);
  EXPECT_THAT(entry->property_name(), "subject[2]");
  content = GetString(&result->document(), "subject[2]");
  EXPECT_THAT(GetMatches(content, *entry), ElementsAre("like"));

  result = &search_results.results(1);
  EXPECT_THAT(result->document().uri(), Eq("uri1"));
  ASSERT_THAT(result->snippet().entries(), SizeIs(3));
  entry = &result->snippet().entries(0);
  EXPECT_THAT(entry->property_name(), "subject[0]");
  content = GetString(&result->document(), "subject[0]");
  EXPECT_THAT(GetMatches(content, *entry), ElementsAre("like"));

  entry = &result->snippet().entries(1);
  ASSERT_THAT(entry->property_name(), "subject[1]");
  content = GetString(&result->document(), "subject[1]");
  EXPECT_THAT(GetMatches(content, *entry), ElementsAre("like"));

  entry = &result->snippet().entries(2);
  ASSERT_THAT(entry->property_name(), "subject[2]");
  content = GetString(&result->document(), "subject[2]");
  EXPECT_THAT(GetMatches(content, *entry), ElementsAre("like"));

  result = &search_results.results(2);
  ASSERT_THAT(result->document().uri(), Eq("uri3"));
  ASSERT_THAT(result->snippet().entries(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, CJKSnippetTest) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // String:     "我每天走路去上班。"
  //              ^ ^  ^   ^^
  // UTF8 idx:    0 3  9  15 18
  // UTF16 idx:   0 1  3   5 6
  // Breaks into segments: "我", "每天", "走路", "去", "上班"
  constexpr std::string_view kChinese = "我每天走路去上班。";
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri1")
                               .SetSchema("Message")
                               .AddStringProperty("body", kChinese)
                               .Build();
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Search and request snippet matching but no windowing.
  SearchSpecProto search_spec;
  search_spec.set_query("走");
  search_spec.set_term_match_type(TERM_MATCH_PREFIX);

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_num_to_snippet(
      std::numeric_limits<int>::max());
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(
      std::numeric_limits<int>::max());

  // Search and make sure that we got a single successful result
  SearchResultProto search_results = icing.Search(
      search_spec, ScoringSpecProto::default_instance(), result_spec);
  ASSERT_THAT(search_results.status(), ProtoIsOk());
  ASSERT_THAT(search_results.results(), SizeIs(1));
  const SearchResultProto::ResultProto* result = &search_results.results(0);
  EXPECT_THAT(result->document().uri(), Eq("uri1"));

  // Ensure that one and only one property was matched and it was "body"
  ASSERT_THAT(result->snippet().entries(), SizeIs(1));
  const SnippetProto::EntryProto* entry = &result->snippet().entries(0);
  EXPECT_THAT(entry->property_name(), Eq("body"));

  // Get the content for "subject" and see what the match is.
  std::string_view content = GetString(&result->document(), "body");
  ASSERT_THAT(content, Eq(kChinese));

  // Ensure that there is one and only one match within "subject"
  ASSERT_THAT(entry->snippet_matches(), SizeIs(1));
  const SnippetMatchProto& match_proto = entry->snippet_matches(0);

  EXPECT_THAT(match_proto.exact_match_byte_position(), Eq(9));
  EXPECT_THAT(match_proto.exact_match_byte_length(), Eq(6));
  std::string_view match =
      content.substr(match_proto.exact_match_byte_position(),
                     match_proto.exact_match_byte_length());
  ASSERT_THAT(match, Eq("走路"));

  // Ensure that the utf-16 values are also as expected
  EXPECT_THAT(match_proto.exact_match_utf16_position(), Eq(3));
  EXPECT_THAT(match_proto.exact_match_utf16_length(), Eq(2));
}

TEST_F(IcingSearchEngineSearchTest, InvalidToEmptyQueryTest) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // String:     "Luca Brasi sleeps with the 🐟🐟🐟."
  //              ^    ^     ^      ^    ^   ^ ^  ^ ^
  // UTF8 idx:    0    5     11     18   23 27 3135 39
  // UTF16 idx:   0    5     11     18   23 27 2931 33
  // Breaks into segments: "Luca", "Brasi", "sleeps", "with", "the", "🐟", "🐟"
  // and "🐟".
  constexpr std::string_view kSicilianMessage =
      "Luca Brasi sleeps with the 🐟🐟🐟.";
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri1")
                               .SetSchema("Message")
                               .AddStringProperty("body", kSicilianMessage)
                               .Build();
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "Some other content.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // Search and request snippet matching but no windowing.
  SearchSpecProto search_spec;
  search_spec.set_query("?");
  search_spec.set_term_match_type(TERM_MATCH_PREFIX);

  ScoringSpecProto scoring_spec;
  ResultSpecProto result_spec;

  // Search and make sure that we got a single successful result
  SearchResultProto search_results =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_results.status(), ProtoIsOk());
  // This is the actual correct behavior.
  EXPECT_THAT(search_results.results(), IsEmpty());

  search_spec.set_query("。");
  search_results = icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_results.status(), ProtoIsOk());
  // This is the actual correct behavior.
  EXPECT_THAT(search_results.results(), IsEmpty());

  search_spec.set_query("-");
  search_results = icing.Search(search_spec, scoring_spec, result_spec);
  // This is the actual correct behavior.
  EXPECT_THAT(search_results.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));

  search_spec.set_query(":");
  search_results = icing.Search(search_spec, scoring_spec, result_spec);
  // This is the actual correct behavior.
  EXPECT_THAT(search_results.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));

  search_spec.set_query("OR");
  search_results = icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_results.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));

  search_spec.set_query(" ");
  search_results = icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_results.status(), ProtoIsOk());
  EXPECT_THAT(search_results.results(), SizeIs(2));
}

TEST_F(IcingSearchEngineSearchTest, EmojiSnippetTest) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // String:     "Luca Brasi sleeps with the 🐟🐟🐟."
  //              ^    ^     ^      ^    ^   ^ ^  ^ ^
  // UTF8 idx:    0    5     11     18   23 27 3135 39
  // UTF16 idx:   0    5     11     18   23 27 2931 33
  // Breaks into segments: "Luca", "Brasi", "sleeps", "with", "the", "🐟", "🐟"
  // and "🐟".
  constexpr std::string_view kSicilianMessage =
      "Luca Brasi sleeps with the 🐟🐟🐟.";
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri1")
                               .SetSchema("Message")
                               .AddStringProperty("body", kSicilianMessage)
                               .Build();
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "Some other content.")
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  // Search and request snippet matching but no windowing.
  SearchSpecProto search_spec;
  search_spec.set_query("🐟");
  search_spec.set_term_match_type(TERM_MATCH_PREFIX);

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_num_to_snippet(1);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(1);

  // Search and make sure that we got a single successful result
  SearchResultProto search_results = icing.Search(
      search_spec, ScoringSpecProto::default_instance(), result_spec);
  ASSERT_THAT(search_results.status(), ProtoIsOk());
  ASSERT_THAT(search_results.results(), SizeIs(1));
  const SearchResultProto::ResultProto* result = &search_results.results(0);
  EXPECT_THAT(result->document().uri(), Eq("uri1"));

  // Ensure that one and only one property was matched and it was "body"
  ASSERT_THAT(result->snippet().entries(), SizeIs(1));
  const SnippetProto::EntryProto* entry = &result->snippet().entries(0);
  EXPECT_THAT(entry->property_name(), Eq("body"));

  // Get the content for "subject" and see what the match is.
  std::string_view content = GetString(&result->document(), "body");
  ASSERT_THAT(content, Eq(kSicilianMessage));

  // Ensure that there is one and only one match within "subject"
  ASSERT_THAT(entry->snippet_matches(), SizeIs(1));
  const SnippetMatchProto& match_proto = entry->snippet_matches(0);

  EXPECT_THAT(match_proto.exact_match_byte_position(), Eq(27));
  EXPECT_THAT(match_proto.exact_match_byte_length(), Eq(4));
  std::string_view match =
      content.substr(match_proto.exact_match_byte_position(),
                     match_proto.exact_match_byte_length());
  ASSERT_THAT(match, Eq("🐟"));

  // Ensure that the utf-16 values are also as expected
  EXPECT_THAT(match_proto.exact_match_utf16_position(), Eq(27));
  EXPECT_THAT(match_proto.exact_match_utf16_length(), Eq(2));
}

TEST_F(IcingSearchEngineSearchTest, JoinByQualifiedId) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("personQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person1")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first1")
          .AddStringProperty("lastName", "last1")
          .AddStringProperty("emailAddress", "email1@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person2")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first2")
          .AddStringProperty("lastName", "last2")
          .AddStringProperty("emailAddress", "email2@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(2)
          .Build();
  DocumentProto person3 =
      DocumentBuilder()
          .SetKey(R"(pkg$db/name#space\\)", "person3")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first3")
          .AddStringProperty("lastName", "last3")
          .AddStringProperty("emailAddress", "email3@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(3)
          .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(3)
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(2)
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("personQualifiedId",
                             R"(pkg$db/name\#space\\\\#person3)")  // escaped
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email3).status(), ProtoIsOk());

  // Parent SearchSpec
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("firstName:first");

  // JoinSpec
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::MAX);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::PREFIX);
  nested_search_spec->set_query("subject:test");

  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  // Parent ScoringSpec
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();

  // Parent ResultSpec
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  // Since we:
  // - Use MAX for aggregation scoring strategy.
  // - (Default) use DOCUMENT_SCORE to score child documents.
  // - (Default) use DESC as the ranking order.
  //
  // person1 + email1 should have the highest aggregated score (3) and be
  // returned first. person2 + email2 (aggregated score = 2) should be the
  // second, and person3 + email3 (aggregated score = 1) should be the last.
  SearchResultProto expected_result1;
  expected_result1.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_proto1 =
      expected_result1.mutable_results()->Add();
  *result_proto1->mutable_document() = person1;
  *result_proto1->mutable_joined_results()->Add()->mutable_document() = email1;

  SearchResultProto expected_result2;
  expected_result2.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_google::protobuf =
      expected_result2.mutable_results()->Add();
  *result_google::protobuf->mutable_document() = person2;
  *result_google::protobuf->mutable_joined_results()->Add()->mutable_document() = email2;

  SearchResultProto expected_result3;
  expected_result3.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_proto3 =
      expected_result3.mutable_results()->Add();
  *result_proto3->mutable_document() = person3;
  *result_proto3->mutable_joined_results()->Add()->mutable_document() = email3;

  SearchResultProto result1 =
      icing.Search(search_spec, scoring_spec, result_spec);
  uint64_t next_page_token = result1.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  expected_result1.set_next_page_token(next_page_token);
  EXPECT_THAT(result1,
              EqualsSearchResultIgnoreStatsAndScores(expected_result1));

  SearchResultProto result2 = icing.GetNextPage(next_page_token);
  next_page_token = result2.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  expected_result2.set_next_page_token(next_page_token);
  EXPECT_THAT(result2,
              EqualsSearchResultIgnoreStatsAndScores(expected_result2));

  SearchResultProto result3 = icing.GetNextPage(next_page_token);
  next_page_token = result3.next_page_token();
  EXPECT_THAT(next_page_token, Eq(kInvalidNextPageToken));
  EXPECT_THAT(result3,
              EqualsSearchResultIgnoreStatsAndScores(expected_result3));
}

TEST_F(IcingSearchEngineSearchTest, JoinByQualifiedIdMultipleNamespaces) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("personQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace1", "person")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first1")
          .AddStringProperty("lastName", "last1")
          .AddStringProperty("emailAddress", "email1@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace2", "person")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first2")
          .AddStringProperty("lastName", "last2")
          .AddStringProperty("emailAddress", "email2@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(2)
          .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace1", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace1#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(3)
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace2", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace1#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(2)
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("namespace2", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace2#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email3).status(), ProtoIsOk());

  // Parent SearchSpec
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("firstName:first");

  // JoinSpec
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::PREFIX);
  nested_search_spec->set_query("subject:test");

  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  // Parent ScoringSpec
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();

  // Parent ResultSpec
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  // Since we:
  // - Use COUNT for aggregation scoring strategy.
  // - (Default) use DESC as the ranking order.
  //
  // pkg$db/namespace1#person + email1, email2 should have the highest
  // aggregated score (2) and be returned first. pkg$db/namespace2#person +
  // email3 (aggregated score = 1) should be the second.
  SearchResultProto expected_result1;
  expected_result1.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_proto1 =
      expected_result1.mutable_results()->Add();
  *result_proto1->mutable_document() = person1;
  *result_proto1->mutable_joined_results()->Add()->mutable_document() = email1;
  *result_proto1->mutable_joined_results()->Add()->mutable_document() = email2;

  SearchResultProto expected_result2;
  expected_result2.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_google::protobuf =
      expected_result2.mutable_results()->Add();
  *result_google::protobuf->mutable_document() = person2;
  *result_google::protobuf->mutable_joined_results()->Add()->mutable_document() = email3;

  SearchResultProto result1 =
      icing.Search(search_spec, scoring_spec, result_spec);
  uint64_t next_page_token = result1.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  expected_result1.set_next_page_token(next_page_token);
  EXPECT_THAT(result1,
              EqualsSearchResultIgnoreStatsAndScores(expected_result1));

  SearchResultProto result2 = icing.GetNextPage(next_page_token);
  next_page_token = result2.next_page_token();
  EXPECT_THAT(next_page_token, Eq(kInvalidNextPageToken));
  EXPECT_THAT(result2,
              EqualsSearchResultIgnoreStatsAndScores(expected_result2));
}

TEST_F(IcingSearchEngineSearchTest,
       JoinShouldLimitNumChildDocumentsByMaxJoinedChildPerParent) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("personQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person1")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first1")
          .AddStringProperty("lastName", "last1")
          .AddStringProperty("emailAddress", "email1@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person2")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first2")
          .AddStringProperty("lastName", "last2")
          .AddStringProperty("emailAddress", "email2@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(2)
          .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(100)
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(99)
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(98)
          .Build();
  DocumentProto email4 =
      DocumentBuilder()
          .SetKey("namespace", "email4")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 4")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(97)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email4).status(), ProtoIsOk());

  // Parent SearchSpec
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("firstName:first");

  // JoinSpec
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::PREFIX);
  nested_search_spec->set_query("subject:test");

  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  // Parent ScoringSpec
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();

  // Parent ResultSpec with max_joined_children_per_parent_to_return = 2
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);
  result_spec.set_max_joined_children_per_parent_to_return(2);

  // - Use COUNT for aggregation scoring strategy.
  // - max_joined_children_per_parent_to_return = 2.
  // - (Default) use DESC as the ranking order.
  //
  // person2 should have the highest aggregated score (3) since email2, email3,
  // email4 are joined to it and the COUNT aggregated score is 3. However, only
  // email2 and email3 should be attached to person2 due to
  // max_joined_children_per_parent_to_return limitation in result_spec.
  // person1 should be the second (aggregated score = 1).
  SearchResultProto::ResultProto expected_result_proto1;
  *expected_result_proto1.mutable_document() = person2;
  expected_result_proto1.set_score(3);
  SearchResultProto::ResultProto* child_result_proto1 =
      expected_result_proto1.mutable_joined_results()->Add();
  *child_result_proto1->mutable_document() = email2;
  child_result_proto1->set_score(99);
  SearchResultProto::ResultProto* child_result_google::protobuf =
      expected_result_proto1.mutable_joined_results()->Add();
  *child_result_google::protobuf->mutable_document() = email3;
  child_result_google::protobuf->set_score(98);

  SearchResultProto::ResultProto expected_result_google::protobuf;
  *expected_result_google::protobuf.mutable_document() = person1;
  expected_result_google::protobuf.set_score(1);
  SearchResultProto::ResultProto* child_result_proto3 =
      expected_result_google::protobuf.mutable_joined_results()->Add();
  *child_result_proto3->mutable_document() = email1;
  child_result_proto3->set_score(100);

  SearchResultProto result1 =
      icing.Search(search_spec, scoring_spec, result_spec);
  uint64_t next_page_token = result1.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  EXPECT_THAT(result1.results(),
              ElementsAre(EqualsProto(expected_result_proto1)));

  SearchResultProto result2 = icing.GetNextPage(next_page_token);
  next_page_token = result2.next_page_token();
  EXPECT_THAT(next_page_token, Eq(kInvalidNextPageToken));
  EXPECT_THAT(result2.results(),
              ElementsAre(EqualsProto(expected_result_google::protobuf)));
}

TEST_F(IcingSearchEngineSearchTest, JoinWithZeroMaxJoinedChildPerParent) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("personQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person1")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first1")
          .AddStringProperty("lastName", "last1")
          .AddStringProperty("emailAddress", "email1@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person2")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first2")
          .AddStringProperty("lastName", "last2")
          .AddStringProperty("emailAddress", "email2@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(2)
          .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(100)
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(99)
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(98)
          .Build();
  DocumentProto email4 =
      DocumentBuilder()
          .SetKey("namespace", "email4")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 4")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(97)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email4).status(), ProtoIsOk());

  // Parent SearchSpec
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("firstName:first");

  // JoinSpec
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::PREFIX);
  nested_search_spec->set_query("subject:test");

  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  // Parent ScoringSpec
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();

  // Parent ResultSpec with max_joined_children_per_parent_to_return = 0
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);
  result_spec.set_max_joined_children_per_parent_to_return(0);

  // - Use COUNT for aggregation scoring strategy.
  // - max_joined_children_per_parent_to_return = 0.
  // - (Default) use DESC as the ranking order.
  //
  // person2 should have the highest aggregated score (3) since email2, email3,
  // email4 are joined to it and the COUNT aggregated score is 3. However, no
  // child documents should be attached to person2 due to
  // max_joined_children_per_parent_to_return limitation in result_spec.
  // person1 should be the second (aggregated score = 1) with no attached child
  // documents.
  SearchResultProto::ResultProto expected_result_proto1;
  *expected_result_proto1.mutable_document() = person2;
  expected_result_proto1.set_score(3);

  SearchResultProto::ResultProto expected_result_google::protobuf;
  *expected_result_google::protobuf.mutable_document() = person1;
  expected_result_google::protobuf.set_score(1);

  SearchResultProto result1 =
      icing.Search(search_spec, scoring_spec, result_spec);
  uint64_t next_page_token = result1.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  EXPECT_THAT(result1.results(),
              ElementsAre(EqualsProto(expected_result_proto1)));

  SearchResultProto result2 = icing.GetNextPage(next_page_token);
  next_page_token = result2.next_page_token();
  EXPECT_THAT(next_page_token, Eq(kInvalidNextPageToken));
  EXPECT_THAT(result2.results(),
              ElementsAre(EqualsProto(expected_result_google::protobuf)));
}

TEST_F(IcingSearchEngineSearchTest, JoinSnippet) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("personQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first")
          .AddStringProperty("lastName", "last")
          .AddStringProperty("emailAddress", "email@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();

  DocumentProto email =
      DocumentBuilder()
          .SetKey("namespace", "email")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(3)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email).status(), ProtoIsOk());

  // Parent SearchSpec
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("firstName:first");

  // JoinSpec
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::MAX);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::PREFIX);
  nested_search_spec->set_query("subject:test");

  // Child ResultSpec (with snippet)
  ResultSpecProto* nested_result_spec = nested_spec->mutable_result_spec();
  nested_result_spec->mutable_snippet_spec()->set_max_window_utf32_length(64);
  nested_result_spec->mutable_snippet_spec()->set_num_matches_per_property(1);
  nested_result_spec->mutable_snippet_spec()->set_num_to_snippet(1);
  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();

  // Parent ScoringSpec
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();

  // Parent ResultSpec (without snippet)
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  SearchResultProto result =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(result.status(), ProtoIsOk());
  EXPECT_THAT(result.next_page_token(), Eq(kInvalidNextPageToken));

  ASSERT_THAT(result.results(), SizeIs(1));
  // Check parent doc (person).
  const DocumentProto& result_parent_document = result.results(0).document();
  EXPECT_THAT(result_parent_document, EqualsProto(person));
  EXPECT_THAT(result.results(0).snippet().entries(), IsEmpty());

  // Check child doc (email).
  ASSERT_THAT(result.results(0).joined_results(), SizeIs(1));
  const DocumentProto& result_child_document =
      result.results(0).joined_results(0).document();
  const SnippetProto& result_child_snippet =
      result.results(0).joined_results(0).snippet();
  EXPECT_THAT(result_child_document, EqualsProto(email));
  ASSERT_THAT(result_child_snippet.entries(), SizeIs(1));
  EXPECT_THAT(result_child_snippet.entries(0).property_name(), Eq("subject"));
  std::string_view content = GetString(
      &result_child_document, result_child_snippet.entries(0).property_name());
  EXPECT_THAT(GetWindows(content, result_child_snippet.entries(0)),
              ElementsAre("test subject"));
  EXPECT_THAT(GetMatches(content, result_child_snippet.entries(0)),
              ElementsAre("test"));
}

TEST_F(IcingSearchEngineSearchTest, JoinProjection) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("personQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first")
          .AddStringProperty("lastName", "last")
          .AddStringProperty("emailAddress", "email@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();

  DocumentProto email =
      DocumentBuilder()
          .SetKey("namespace", "email")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(3)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email).status(), ProtoIsOk());

  // Parent SearchSpec
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("firstName:first");

  // JoinSpec
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::MAX);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::PREFIX);
  nested_search_spec->set_query("subject:test");

  // Child ResultSpec (with projection)
  ResultSpecProto* nested_result_spec = nested_spec->mutable_result_spec();
  TypePropertyMask* type_property_mask =
      nested_result_spec->add_type_property_masks();
  type_property_mask->set_schema_type("Email");
  type_property_mask->add_paths("subject");
  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();

  // Parent ScoringSpec
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();

  // Parent ResultSpec (with projection)
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());
  type_property_mask = result_spec.add_type_property_masks();
  type_property_mask->set_schema_type("Person");
  type_property_mask->add_paths("emailAddress");

  SearchResultProto result =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(result.status(), ProtoIsOk());
  EXPECT_THAT(result.next_page_token(), Eq(kInvalidNextPageToken));

  ASSERT_THAT(result.results(), SizeIs(1));
  // Check parent doc (person): should contain only the "emailAddress" property.
  DocumentProto projected_person_document =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("emailAddress", "email@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(1)
          .Build();
  EXPECT_THAT(result.results().at(0).document(),
              EqualsProto(projected_person_document));

  // Check child doc (email): should contain only the "subject" property.
  ASSERT_THAT(result.results(0).joined_results(), SizeIs(1));
  DocumentProto projected_email_document =
      DocumentBuilder()
          .SetKey("namespace", "email")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(3)
          .Build();
  EXPECT_THAT(result.results(0).joined_results(0).document(),
              EqualsProto(projected_email_document));
}

TEST_F(IcingSearchEngineSearchTest, JoinWithAdvancedScoring) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("personQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  const int32_t person1_doc_score = 10;
  const int32_t person2_doc_score = 25;
  const int32_t person3_doc_score = 123;
  const int32_t email1_doc_score = 10;
  const int32_t email2_doc_score = 15;
  const int32_t email3_doc_score = 40;

  // person1 has children email1 and email2.
  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person1")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first1")
          .AddStringProperty("lastName", "last1")
          .AddStringProperty("emailAddress", "email1@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(person1_doc_score)
          .Build();
  // person2 has a single child email3
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person2")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first2")
          .AddStringProperty("lastName", "last2")
          .AddStringProperty("emailAddress", "email2@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(person2_doc_score)
          .Build();
  // person3 has no child.
  DocumentProto person3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "person3")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first3")
          .AddStringProperty("lastName", "last3")
          .AddStringProperty("emailAddress", "email3@gmail.com")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(person3_doc_score)
          .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(email1_doc_score)
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(email2_doc_score)
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("personQualifiedId", "pkg$db/namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetScore(email3_doc_score)
          .Build();

  // Set children scoring expression and their expected value.
  ScoringSpecProto child_scoring_spec = GetDefaultScoringSpec();
  child_scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  child_scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() * 2 + 1");
  child_scoring_spec.add_additional_advanced_scoring_expressions(
      "this.documentScore()");
  const int32_t exp_email1_score = email1_doc_score * 2 + 1;
  const int32_t exp_email2_score = email2_doc_score * 2 + 1;
  const int32_t exp_email3_score = email3_doc_score * 2 + 1;

  // Set parent scoring expression and their expected value.
  ScoringSpecProto parent_scoring_spec = GetDefaultScoringSpec();
  parent_scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  parent_scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() * sum(this.childrenRankingSignals())");
  const int32_t exp_person1_score =
      person1_doc_score * (exp_email1_score + exp_email2_score);
  const int32_t exp_person2_score = person2_doc_score * exp_email3_score;
  const int32_t exp_person3_score = person3_doc_score * 0;

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email3).status(), ProtoIsOk());

  // Parent SearchSpec
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("firstName:first");

  // JoinSpec
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::PREFIX);
  nested_search_spec->set_query("subject:test");
  *nested_spec->mutable_scoring_spec() = child_scoring_spec;
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  // Parent ResultSpec
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  SearchResultProto results =
      icing.Search(search_spec, parent_scoring_spec, result_spec);
  uint64_t next_page_token = results.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  ASSERT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document().uri(), Eq("person2"));
  // exp_person2_score = 2025
  EXPECT_THAT(results.results(0).score(), Eq(exp_person2_score));
  EXPECT_THAT(results.results(0).joined_results_size(), Eq(1));
  EXPECT_THAT(results.results(0).joined_results(0).document().uri(),
              Eq("email3"));
  EXPECT_THAT(results.results(0).joined_results(0).score(),
              Eq(exp_email3_score));
  EXPECT_THAT(results.results(0).joined_results(0).additional_scores(),
              ElementsAre(email3_doc_score));

  results = icing.GetNextPage(next_page_token);
  next_page_token = results.next_page_token();
  EXPECT_THAT(next_page_token, Ne(kInvalidNextPageToken));
  ASSERT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document().uri(), Eq("person1"));
  // exp_person1_score = 520
  EXPECT_THAT(results.results(0).score(), Eq(exp_person1_score));
  EXPECT_THAT(results.results(0).joined_results_size(), Eq(2));
  EXPECT_THAT(results.results(0).joined_results(0).document().uri(),
              Eq("email2"));
  EXPECT_THAT(results.results(0).joined_results(0).score(),
              Eq(exp_email2_score));
  EXPECT_THAT(results.results(0).joined_results(0).additional_scores(),
              ElementsAre(email2_doc_score));
  EXPECT_THAT(results.results(0).joined_results(1).document().uri(),
              Eq("email1"));
  EXPECT_THAT(results.results(0).joined_results(1).score(),
              Eq(exp_email1_score));
  EXPECT_THAT(results.results(0).joined_results(1).additional_scores(),
              ElementsAre(email1_doc_score));

  results = icing.GetNextPage(next_page_token);
  next_page_token = results.next_page_token();
  EXPECT_THAT(next_page_token, Eq(kInvalidNextPageToken));
  ASSERT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document().uri(), Eq("person3"));
  // exp_person3_score = 0
  EXPECT_THAT(results.results(0).score(), Eq(exp_person3_score));
  EXPECT_THAT(results.results(0).joined_results(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, NumericFilterAdvancedQuerySucceeds) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("transaction")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("price")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("cost")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  DocumentProto document_one = DocumentBuilder()
                                   .SetKey("namespace", "1")
                                   .SetSchema("transaction")
                                   .SetCreationTimestampMs(1)
                                   .AddInt64Property("price", 10)
                                   .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two = DocumentBuilder()
                                   .SetKey("namespace", "2")
                                   .SetSchema("transaction")
                                   .SetCreationTimestampMs(1)
                                   .AddInt64Property("price", 25)
                                   .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  DocumentProto document_three = DocumentBuilder()
                                     .SetKey("namespace", "3")
                                     .SetSchema("transaction")
                                     .SetCreationTimestampMs(1)
                                     .AddInt64Property("cost", 2)
                                     .Build();
  ASSERT_THAT(icing.Put(document_three).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("price < 20");
  search_spec.add_enabled_features(std::string(kNumericSearchFeature));

  SearchResultProto results =
      icing.Search(search_spec, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  ASSERT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_one));

  search_spec.set_query("price == 25");
  results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                         ResultSpecProto::default_instance());
  ASSERT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));

  search_spec.set_query("cost > 2");
  results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.results(), IsEmpty());

  search_spec.set_query("cost >= 2");
  results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                         ResultSpecProto::default_instance());
  ASSERT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_three));

  search_spec.set_query("price <= 25");
  results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                         ResultSpecProto::default_instance());
  ASSERT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));
}

TEST_F(IcingSearchEngineSearchTest,
       NumericFilterAdvancedQueryWithPersistenceSucceeds) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Create the schema and document store
    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("transaction")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("price")
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("cost")
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();

    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    // Schema will be persisted to disk when icing goes out of scope.
  }

  DocumentProto document_one = DocumentBuilder()
                                   .SetKey("namespace", "1")
                                   .SetSchema("transaction")
                                   .SetCreationTimestampMs(1)
                                   .AddInt64Property("price", 10)
                                   .Build();
  DocumentProto document_two = DocumentBuilder()
                                   .SetKey("namespace", "2")
                                   .SetSchema("transaction")
                                   .SetCreationTimestampMs(1)
                                   .AddInt64Property("price", 25)
                                   .AddInt64Property("cost", 2)
                                   .Build();
  {
    // Ensure that icing initializes the schema and section_manager
    // properly from the pre-existing file.
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());
    // The index and document store will be persisted to disk when icing goes
    // out of scope.
  }

  {
    // Ensure that the index is brought back up without problems and we
    // can query for the content that we expect.
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    SearchSpecProto search_spec;
    search_spec.set_query("price < 20");
    search_spec.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results.results(), SizeIs(1));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document_one));

    search_spec.set_query("price == 25");
    results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                           ResultSpecProto::default_instance());
    ASSERT_THAT(results.results(), SizeIs(1));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));

    search_spec.set_query("cost > 2");
    results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                           ResultSpecProto::default_instance());
    EXPECT_THAT(results.results(), IsEmpty());

    search_spec.set_query("cost >= 2");
    results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                           ResultSpecProto::default_instance());
    ASSERT_THAT(results.results(), SizeIs(1));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));

    search_spec.set_query("price <= 25");
    results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                           ResultSpecProto::default_instance());
    ASSERT_THAT(results.results(), SizeIs(2));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));
    EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));
  }
}

TEST_F(IcingSearchEngineSearchTest, NumericFilterQueryStatsProtoTest) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(5);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Create the schema and document store
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("transaction")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("price")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("cost")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  DocumentProto document_one = DocumentBuilder()
                                   .SetKey("namespace", "1")
                                   .SetSchema("transaction")
                                   .SetCreationTimestampMs(1)
                                   .AddInt64Property("price", 10)
                                   .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());

  DocumentProto document_two = DocumentBuilder()
                                   .SetKey("namespace", "2")
                                   .SetSchema("transaction")
                                   .SetCreationTimestampMs(2)
                                   .AddInt64Property("price", 25)
                                   .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  DocumentProto document_three = DocumentBuilder()
                                     .SetKey("namespace", "3")
                                     .SetSchema("transaction")
                                     .SetCreationTimestampMs(3)
                                     .AddInt64Property("cost", 2)
                                     .Build();
  ASSERT_THAT(icing.Put(document_three).status(), ProtoIsOk());

  DocumentProto document_four = DocumentBuilder()
                                    .SetKey("namespace", "3")
                                    .SetSchema("transaction")
                                    .SetCreationTimestampMs(4)
                                    .AddInt64Property("price", 15)
                                    .Build();
  ASSERT_THAT(icing.Put(document_four).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.add_namespace_filters("namespace");
  search_spec.add_schema_type_filters(document_one.schema());
  search_spec.set_query("price < 20");
  search_spec.add_enabled_features(std::string(kNumericSearchFeature));

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(5);

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);

  SearchResultProto results =
      icing.Search(search_spec, scoring_spec, result_spec);
  ASSERT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_four));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));

  // Check the stats
  // TODO(b/305098009): deprecate search-related flat fields in query_stats.
  QueryStatsProto exp_stats;
  exp_stats.set_query_length(10);
  exp_stats.set_num_terms(0);
  exp_stats.set_num_namespaces_filtered(1);
  exp_stats.set_num_schema_types_filtered(1);
  exp_stats.set_ranking_strategy(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);
  exp_stats.set_is_first_page(true);
  exp_stats.set_requested_page_size(5);
  exp_stats.set_num_results_returned_current_page(2);
  exp_stats.set_num_documents_scored(2);
  exp_stats.set_num_results_with_snippets(0);
  exp_stats.set_latency_ms(5);
  exp_stats.set_parse_query_latency_ms(5);
  exp_stats.set_scoring_latency_ms(5);
  exp_stats.set_ranking_latency_ms(5);
  exp_stats.set_document_retrieval_latency_ms(5);
  exp_stats.set_lock_acquisition_latency_ms(5);
  exp_stats.set_num_joined_results_returned_current_page(0);
  exp_stats.set_lite_index_hit_buffer_byte_size(0);
  exp_stats.set_lite_index_hit_buffer_unsorted_byte_size(0);

  QueryStatsProto::SearchStats* exp_parent_search_stats =
      exp_stats.mutable_parent_search_stats();
  exp_parent_search_stats->set_query_length(10);
  exp_parent_search_stats->set_num_terms(0);
  exp_parent_search_stats->set_num_namespaces_filtered(1);
  exp_parent_search_stats->set_num_schema_types_filtered(1);
  exp_parent_search_stats->set_ranking_strategy(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);
  exp_parent_search_stats->set_is_numeric_query(true);
  exp_parent_search_stats->set_num_documents_scored(2);
  exp_parent_search_stats->set_parse_query_latency_ms(5);
  exp_parent_search_stats->set_scoring_latency_ms(5);
  exp_parent_search_stats->set_num_fetched_hits_lite_index(0);
  exp_parent_search_stats->set_num_fetched_hits_main_index(0);
  // Since we will inspect 1 bucket from "price" in integer index and it
  // contains 3 hits, we will fetch 3 hits (but filter out one of them).
  exp_parent_search_stats->set_num_fetched_hits_integer_index(3);
  exp_parent_search_stats->set_query_processor_lexer_extract_token_latency_ms(
      5);
  exp_parent_search_stats->set_query_processor_parser_consume_query_latency_ms(
      5);
  exp_parent_search_stats->set_query_processor_query_visitor_latency_ms(5);

  EXPECT_THAT(results.query_stats(), EqualsProto(exp_stats));
}

TEST_F(IcingSearchEngineSearchTest, BarisNormalizationTest) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri")
                               .SetSchema("Person")
                               .SetCreationTimestampMs(1)
                               .AddStringProperty("name", "Barış")
                               .Build();
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  DocumentProto document_two = DocumentBuilder()
                                   .SetKey("namespace", "uri2")
                                   .SetSchema("Person")
                                   .SetCreationTimestampMs(1)
                                   .AddStringProperty("name", "ıbar")
                                   .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TERM_MATCH_PREFIX);

  ScoringSpecProto scoring_spec;
  ResultSpecProto result_spec;

  SearchResultProto exp_results;
  exp_results.mutable_status()->set_code(StatusProto::OK);
  *exp_results.add_results()->mutable_document() = document;

  search_spec.set_query("barış");
  SearchResultProto results =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(results, EqualsSearchResultIgnoreStatsAndScores(exp_results));

  search_spec.set_query("barıs");
  results = icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(results, EqualsSearchResultIgnoreStatsAndScores(exp_results));

  search_spec.set_query("baris");
  results = icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(results, EqualsSearchResultIgnoreStatsAndScores(exp_results));

  SearchResultProto exp_results2;
  exp_results2.mutable_status()->set_code(StatusProto::OK);
  *exp_results2.add_results()->mutable_document() = document_two;
  search_spec.set_query("ı");
  results = icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(results, EqualsSearchResultIgnoreStatsAndScores(exp_results2));
}

TEST_F(IcingSearchEngineSearchTest, LatinSnippetTest) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  constexpr std::string_view kLatin = "test ḞÖÖḸĬŞĤ test";
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri1")
                               .SetSchema("Message")
                               .AddStringProperty("body", kLatin)
                               .Build();
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TERM_MATCH_PREFIX);

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_num_to_snippet(
      std::numeric_limits<int>::max());
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(
      std::numeric_limits<int>::max());

  // Search and make sure that we got a single successful result
  SearchResultProto search_results = icing.Search(
      search_spec, ScoringSpecProto::default_instance(), result_spec);
  ASSERT_THAT(search_results.status(), ProtoIsOk());
  ASSERT_THAT(search_results.results(), SizeIs(1));
  const SearchResultProto::ResultProto* result = &search_results.results(0);
  EXPECT_THAT(result->document().uri(), Eq("uri1"));

  // Ensure that one and only one property was matched and it was "body"
  ASSERT_THAT(result->snippet().entries(), SizeIs(1));
  const SnippetProto::EntryProto* entry = &result->snippet().entries(0);
  EXPECT_THAT(entry->property_name(), Eq("body"));

  // Ensure that there is one and only one match within "body"
  ASSERT_THAT(entry->snippet_matches(), SizeIs(1));

  // Check that the match is "ḞÖÖḸĬŞĤ".
  const SnippetMatchProto& match_proto = entry->snippet_matches(0);
  std::string_view match =
      kLatin.substr(match_proto.exact_match_byte_position(),
                    match_proto.submatch_byte_length());
  ASSERT_THAT(match, Eq("ḞÖÖ"));
}

TEST_F(IcingSearchEngineSearchTest,
       DocumentStoreNamespaceIdFingerprintCompatible) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");

  // Initialize with some documents with document_store_namespace_id_fingerprint
  // being false.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_document_store_namespace_id_fingerprint(false);
    IcingSearchEngine icing(options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

    // Creates and inserts 3 documents
    ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());
  }

  // Reinitializate with document_store_namespace_id_fingerprint being true,
  // and test that we are still able to read/query docs.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_document_store_namespace_id_fingerprint(true);
    IcingSearchEngine icing(options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    ASSERT_THAT(
        icing.Get("namespace", "uri1", GetResultSpecProto::default_instance())
            .status(),
        ProtoIsOk());
    ASSERT_THAT(
        icing.Get("namespace", "uri2", GetResultSpecProto::default_instance())
            .status(),
        ProtoIsOk());
    ASSERT_THAT(
        icing.Get("namespace", "uri3", GetResultSpecProto::default_instance())
            .status(),
        ProtoIsOk());

    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::PREFIX);
    search_spec.set_query("message");

    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results.results(), SizeIs(3));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document3));
    EXPECT_THAT(results.results(1).document(), EqualsProto(document2));
    EXPECT_THAT(results.results(2).document(), EqualsProto(document1));
  }

  // Reinitializate with document_store_namespace_id_fingerprint being false,
  // and test that we are still able to read/query docs.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_document_store_namespace_id_fingerprint(false);
    IcingSearchEngine icing(options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    ASSERT_THAT(
        icing.Get("namespace", "uri1", GetResultSpecProto::default_instance())
            .status(),
        ProtoIsOk());
    ASSERT_THAT(
        icing.Get("namespace", "uri2", GetResultSpecProto::default_instance())
            .status(),
        ProtoIsOk());
    ASSERT_THAT(
        icing.Get("namespace", "uri3", GetResultSpecProto::default_instance())
            .status(),
        ProtoIsOk());

    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::PREFIX);
    search_spec.set_query("message");

    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results.results(), SizeIs(3));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document3));
    EXPECT_THAT(results.results(1).document(), EqualsProto(document2));
    EXPECT_THAT(results.results(2).document(), EqualsProto(document1));
  }
}

TEST_F(IcingSearchEngineSearchTest, HasPropertyQuery) {
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

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_build_property_existence_metadata_hits(true);
  IcingSearchEngine icing(options, GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document0).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

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
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document2));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document0));

  // Get all documents that have "timestamp".
  search_spec.set_query("hasProperty(\"timestamp\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document0));

  // Get all documents that have "score".
  search_spec.set_query("hasProperty(\"score\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(3));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document2));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(2).document(), EqualsProto(document0));
}

TEST_F(IcingSearchEngineSearchTest,
       HasPropertyQueryDoesNotWorkWithoutMetadataHits) {
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

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_build_property_existence_metadata_hits(false);
  IcingSearchEngine icing(options, GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document0).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // Check that none of the following hasProperty queries can return any
  // results.
  //
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
  EXPECT_THAT(results.results(), IsEmpty());

  // Get all documents that have "timestamp".
  search_spec.set_query("hasProperty(\"timestamp\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), IsEmpty());

  // Get all documents that have "score".
  search_spec.set_query("hasProperty(\"score\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, HasPropertyQueryNestedDocument) {
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
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("TreeNode")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("value")
                               .SetDataTypeDocument(
                                   "Value", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Create a complex nested root_document with the following property paths.
  // - name
  // - value
  // - value.body
  // - value.score
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "uri")
          .SetSchema("TreeNode")
          .SetCreationTimestampMs(1)
          .AddStringProperty("name", "root")
          .AddDocumentProperty("value", DocumentBuilder()
                                            .SetKey("icing", "uri")
                                            .SetSchema("Value")
                                            .AddStringProperty("body", "foo")
                                            .AddDoubleProperty("score", 456.789)
                                            .Build())
          .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_build_property_existence_metadata_hits(true);
  IcingSearchEngine icing(options, GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Check that the document can be found by `hasProperty("name")`.
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  search_spec.add_enabled_features(std::string(kHasPropertyFunctionFeature));
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  search_spec.set_query("hasProperty(\"name\")");
  SearchResultProto results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document));

  // Check that the document can be found by `hasProperty("value")`.
  search_spec.set_query("hasProperty(\"value\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document));

  // Check that the document can be found by `hasProperty("value.body")`.
  search_spec.set_query("hasProperty(\"value.body\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document));

  // Check that the document can be found by `hasProperty("value.score")`.
  search_spec.set_query("hasProperty(\"value.score\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document));

  // Check that the document can NOT be found by `hasProperty("body")`.
  search_spec.set_query("hasProperty(\"body\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), IsEmpty());

  // Check that the document can NOT be found by `hasProperty("score")`.
  search_spec.set_query("hasProperty(\"score\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, EmbeddingSearch) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding1")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding2")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 =
      DocumentBuilder()
          .SetKey("icing", "uri0")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddStringProperty("body", "foo")
          .AddVectorProperty(
              "embedding1",
              CreateVector("my_model_v1", {0.1, 0.2, 0.3, 0.4, 0.5}))
          .AddVectorProperty(
              "embedding2",
              CreateVector("my_model_v1", {-0.1, -0.2, -0.3, 0.4, 0.5}),
              CreateVector("my_model_v2", {0.6, 0.7, 0.8}))
          .Build();
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("icing", "uri1")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddVectorProperty(
              "embedding1",
              CreateVector("my_model_v1", {-0.1, 0.2, -0.3, -0.4, 0.5}))
          .AddVectorProperty("embedding2",
                             CreateVector("my_model_v2", {0.6, 0.7, -0.8}))
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document0).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));

  // Add an embedding query with semantic scores:
  // - document 0: -0.5 (embedding1), 0.3 (embedding2)
  // - document 1: -0.9 (embedding1)
  *search_spec.add_embedding_query_vectors() =
      CreateVector("my_model_v1", {1, -1, -1, 1, -1});
  // Add an embedding query with semantic scores:
  // - document 0: -0.5 (embedding2)
  // - document 1: -2.1 (embedding2)
  *search_spec.add_embedding_query_vectors() =
      CreateVector("my_model_v2", {-1, -1, 1});
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);

  // Match documents that have embeddings with a similarity closer to 0 that is
  // greater than -1.
  //
  // The matched embeddings for each doc are:
  // - document 0: -0.5 (embedding1), 0.3 (embedding2)
  // - document 1: -0.9 (embedding1)
  // The scoring expression for each doc will be evaluated as:
  // - document 0: sum({-0.5, 0.3}) + sum({}) = -0.2
  // - document 1: sum({-0.9}) + sum({}) = -0.9
  search_spec.set_query("semanticSearch(getEmbeddingParameter(0), -1)");
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))");
  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document0));
  EXPECT_THAT(results.results(0).score(), DoubleNear(-0.5 + 0.3, kEps));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(1).score(), DoubleNear(-0.9, kEps));

  // Create a query the same as above but with a section restriction, which
  // still matches document 0 and document 1 but the semantic score 0.3 should
  // be removed from document 0.
  //
  // The matched embeddings for each doc are:
  // - document 0: -0.5 (embedding1)
  // - document 1: -0.9 (embedding1)
  // The scoring expression for each doc will be evaluated as:
  // - document 0: sum({-0.5}) = -0.5
  // - document 1: sum({-0.9}) = -0.9
  search_spec.set_query(
      "embedding1:semanticSearch(getEmbeddingParameter(0), -1)");
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))");
  results = icing.Search(search_spec, scoring_spec,
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document0));
  EXPECT_THAT(results.results(0).score(), DoubleNear(-0.5, kEps));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(1).score(), DoubleNear(-0.9, kEps));

  // Create a query that only matches document 0.
  //
  // The matched embeddings for each doc are:
  // - document 0: -0.5 (embedding2)
  // The scoring expression for each doc will be evaluated as:
  // - document 0: sum({-0.5}) = -0.5
  search_spec.set_query("semanticSearch(getEmbeddingParameter(1), -1.5)");
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(1)))");
  results = icing.Search(search_spec, scoring_spec,
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document0));
  EXPECT_THAT(results.results(0).score(), DoubleNear(-0.5, kEps));

  // Create a query that only matches document 1.
  //
  // The matched embeddings for each doc are:
  // - document 1: -2.1 (embedding2)
  // The scoring expression for each doc will be evaluated as:
  // - document 1: sum({-2.1}) = -2.1
  search_spec.set_query("semanticSearch(getEmbeddingParameter(1), -10, -1)");
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(1)))");
  results = icing.Search(search_spec, scoring_spec,
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(0).score(), DoubleNear(-2.1, kEps));

  // Create a complex query that matches all hits from all documents.
  //
  // The matched embeddings for each doc are:
  // - document 0: -0.5 (embedding1), 0.3 (embedding2), -0.5 (embedding2)
  // - document 1: -0.9 (embedding1), -2.1 (embedding2)
  // The scoring expression for each doc will be evaluated as:
  // - document 0: sum({-0.5, 0.3}) + sum({-0.5}) = -0.7
  // - document 1: sum({-0.9}) + sum({-2.1}) = -3
  search_spec.set_query(
      "semanticSearch(getEmbeddingParameter(0)) OR "
      "semanticSearch(getEmbeddingParameter(1))");
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(0))) + "
      "sum(this.matchedSemanticScores(getEmbeddingParameter(1)))");
  results = icing.Search(search_spec, scoring_spec,
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document0));
  EXPECT_THAT(results.results(0).score(), DoubleNear(-0.5 + 0.3 - 0.5, kEps));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(1).score(), DoubleNear(-0.9 - 2.1, kEps));

  // Create a hybrid query that matches document 0 because of term-based search
  // and document 1 because of embedding-based search.
  //
  // The matched embeddings for each doc are:
  // - document 1: -2.1 (embedding2)
  // The scoring expression for each doc will be evaluated as:
  // - document 0: sum({}) = 0
  // - document 1: sum({-2.1}) = -2.1
  search_spec.set_query(
      "foo OR semanticSearch(getEmbeddingParameter(1), -10, -1)");
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(1)))");
  results = icing.Search(search_spec, scoring_spec,
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document0));
  // Document 0 has no matched embedding hit, so its score is 0.
  EXPECT_THAT(results.results(0).score(), DoubleNear(0, kEps));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(1).score(), DoubleNear(-2.1, kEps));
}

TEST_F(IcingSearchEngineSearchTest, CannotScoreUnqueriedEmbedding) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "uri0")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddStringProperty("body", "foo")
          .AddVectorProperty(
              "embedding", CreateVector("my_model", {0.1, 0.2, 0.3, 0.4, 0.5}))
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  *search_spec.add_embedding_query_vectors() =
      CreateVector("my_model", {1, -1, -1, 1, -1});
  *search_spec.add_embedding_query_vectors() =
      CreateVector("my_model", {-1, -1, 1});
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);

  // Query with DOT_PRODUCT but score with COSINE.
  search_spec.set_query(
      "semanticSearch(getEmbeddingParameter(0), -1, 1, \"DOT_PRODUCT\")");
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(0), \"COSINE\"))");
  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
  EXPECT_THAT(results.status().message(),
              HasSubstr("embedding query index 0 with metric type COSINE has "
                        "not been queried"));

  // Query with embedding index 0 but score with embedding index 1.
  search_spec.set_query("semanticSearch(getEmbeddingParameter(0), -1, 1)");
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(1)))");
  results = icing.Search(search_spec, scoring_spec,
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
  EXPECT_THAT(results.status().message(),
              HasSubstr("embedding query index 1 with metric type DOT_PRODUCT "
                        "has not been queried"));
}

TEST_F(IcingSearchEngineSearchTest, AdditionalScores) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 =
      DocumentBuilder()
          .SetKey("icing", "uri0")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddStringProperty("body", "foo")
          .AddVectorProperty(
              "embedding",
              CreateVector("my_model", {-0.1, 0.2, -0.3, -0.4, 0.5}))
          .Build();
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("icing", "uri1")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddVectorProperty(
              "embedding", CreateVector("my_model", {0.1, 0.2, 0.3, 0.4, 0.5}))
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document0).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query(
      "foo OR semanticSearch(getEmbeddingParameter(0), 0, 1)");
  // Add an embedding query with semantic scores:
  // - document 0: 0.9 (embedding)
  // - document 1: 0.5 (embedding)
  *search_spec.add_embedding_query_vectors() =
      CreateVector("my_model", {-1, 1, 1, -1, 1});
  search_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));

  // Create a scoring spec that:
  // - Uses sum(this.matchedSemanticScores(getEmbeddingParameter(0))) for
  //   ranking.
  // - Configures the following additional scores:
  //   - this.relevanceScore()
  //   - this.relevanceScore() +
  //     sum(this.matchedSemanticScores(getEmbeddingParameter(1)))
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))");
  scoring_spec.add_additional_advanced_scoring_expressions(
      "this.relevanceScore()");
  scoring_spec.add_additional_advanced_scoring_expressions(
      "this.relevanceScore() + "
      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))");
  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  // Check results for document 0.
  EXPECT_THAT(results.results(0).document(), EqualsProto(document0));
  EXPECT_THAT(results.results(0).score(), DoubleNear(0.9, kEps));
  EXPECT_THAT(results.results(0).additional_scores(), SizeIs(2));
  // this.relevanceScore() is 0.3930216 for document 0.
  const double relevance_score_0 = 0.3930216;
  EXPECT_THAT(results.results(0).additional_scores(0),
              DoubleNear(relevance_score_0, kEps));
  EXPECT_THAT(results.results(0).additional_scores(1),
              DoubleNear(relevance_score_0 + 0.9, kEps));

  // Check results for document 1.
  EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(1).score(), DoubleNear(0.5, kEps));
  EXPECT_THAT(results.results(1).additional_scores(), SizeIs(2));
  // this.relevanceScore() is 0 for document 1.
  EXPECT_THAT(results.results(1).additional_scores(0), DoubleNear(0, kEps));
  EXPECT_THAT(results.results(1).additional_scores(1),
              DoubleNear(0 + 0.5, kEps));
}

TEST_F(IcingSearchEngineSearchTest, EmbeddingSearchWithQuantizedProperty) {
  constexpr float eps = 0.0001f;

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embeddingQuantized")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH,
                                            QUANTIZATION_TYPE_QUANTIZE_8_BIT)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  // If quantization is enabled, this vector will be quantized to {0, 1, 255}.
  PropertyProto::VectorProto vector1 = CreateVector("my_model", {0, 1.45, 255});
  // If quantization is enabled, this vector will be quantized to {0, -2, -255}.
  PropertyProto::VectorProto vector2 =
      CreateVector("my_model", {0, -2.15, -255});

  DocumentProto document_with_original_embedding =
      DocumentBuilder()
          .SetKey("icing", "uri0")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddStringProperty("body", "foo")
          .AddVectorProperty("embedding", vector1, vector2)
          .Build();
  DocumentProto document_with_quantized_embedding =
      DocumentBuilder()
          .SetKey("icing", "uri1")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddVectorProperty("embeddingQuantized", vector1, vector2)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document_with_original_embedding).status(),
              ProtoIsOk());
  ASSERT_THAT(icing.Put(document_with_quantized_embedding).status(),
              ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  // Add an embedding query with semantic scores:
  // - [0 + 1.45 + 255 = 256.45, 0 - 2.15 - 255 = -257.15] for
  //   document_with_original_embedding
  // - [0 + 1 + 255 = 256, 0 - 2 - 255 = -257] for
  //   document_with_quantized_embedding
  *search_spec.add_embedding_query_vectors() =
      CreateVector("my_model", {1, 1, 1});
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);

  // All embeddings should be matched for the range of [-1000, 1000].
  search_spec.set_query(
      "semanticSearch(getEmbeddingParameter(0), -1000, 1000)");
  scoring_spec.set_advanced_scoring_expression(
      "len(this.matchedSemanticScores(getEmbeddingParameter(0)))");
  scoring_spec.add_additional_advanced_scoring_expressions(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))");
  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  // Check results for document_with_quantized_embedding.
  EXPECT_THAT(results.results(0).document(),
              EqualsProto(document_with_quantized_embedding));
  EXPECT_THAT(results.results(0).score(), 2);
  EXPECT_THAT(results.results(0).additional_scores(0),
              DoubleNear(256 + (-257), eps));
  // Check results for document_with_original_embedding.
  EXPECT_THAT(results.results(1).document(),
              EqualsProto(document_with_original_embedding));
  EXPECT_THAT(results.results(1).score(), 2);
  EXPECT_THAT(results.results(1).additional_scores(0),
              DoubleNear(256.45 + (-257.15), eps));

  // Only one embedding (with score of 256.45, or 256 if quantized) should be
  // matched for the range of [0, 1000].
  search_spec.set_query("semanticSearch(getEmbeddingParameter(0), 0, 1000)");
  scoring_spec.set_advanced_scoring_expression(
      "len(this.matchedSemanticScores(getEmbeddingParameter(0)))");
  scoring_spec.add_additional_advanced_scoring_expressions(
      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))");
  results = icing.Search(search_spec, scoring_spec,
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  // Check results for document_with_quantized_embedding.
  EXPECT_THAT(results.results(0).document(),
              EqualsProto(document_with_quantized_embedding));
  EXPECT_THAT(results.results(0).score(), 1);
  EXPECT_THAT(results.results(0).additional_scores(0), DoubleNear(256, eps));
  // Check results for document_with_original_embedding.
  EXPECT_THAT(results.results(1).document(),
              EqualsProto(document_with_original_embedding));
  EXPECT_THAT(results.results(1).score(), 1);
  EXPECT_THAT(results.results(1).additional_scores(0), DoubleNear(256.45, eps));
}

TEST_F(IcingSearchEngineSearchTest,
       AdditionalScoresOnlyAllowedInAdvancedScoring) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Email").AddProperty(
              PropertyConfigBuilder()
                  .SetName("body")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "uri0")
                               .SetSchema("Email")
                               .SetCreationTimestampMs(1)
                               .AddStringProperty("body", "foo")
                               .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("foo");

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  scoring_spec.add_additional_advanced_scoring_expressions(
      "this.relevanceScore()");
  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest,
       EmbeddingSearchWithManyFilteredOutDocuments) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Email").AddProperty(
              PropertyConfigBuilder()
                  .SetName("embedding")
                  .SetDataTypeVector(EMBEDDING_INDEXING_LINEAR_SEARCH)
                  .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  for (int i = 0; i < 50000; ++i) {
    DocumentProto document =
        DocumentBuilder()
            .SetKey("icing", "uri" + std::to_string(i))
            .SetSchema("Email")
            .SetCreationTimestampMs(1)
            .AddVectorProperty("embedding",
                               CreateVector("my_model", {0.1, 0.2, 0.3}))
            .Build();
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));

  // Create an embedding query with a range that should not match any embedding
  // hits.
  *search_spec.add_embedding_query_vectors() =
      CreateVector("my_model", {1, 1, 1});
  search_spec.set_query("semanticSearch(getEmbeddingParameter(0), 100)");

  SearchResultProto results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, SearchWithPropertyFiltersEmbedding) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding1")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding2")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding3")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 =
      DocumentBuilder()
          .SetKey("icing", "uri0")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddStringProperty("body", "foo")
          .AddVectorProperty("embedding1",
                             CreateVector("my_model", {0.1, 0.2, 0.3}))
          .Build();
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("icing", "uri1")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddStringProperty("body", "foo")
          .AddVectorProperty("embedding2",
                             CreateVector("my_model", {-0.1, -0.2, -0.3}))
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("icing", "uri2")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddStringProperty("body", "foo")
          .AddVectorProperty("embedding3",
                             CreateVector("my_model", {1.0, 2.0, 3.0}))
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("icing", "uri3")
          .SetSchema("Email")
          .SetCreationTimestampMs(1)
          .AddStringProperty("body", "foo")
          .AddVectorProperty("embedding1",
                             CreateVector("my_model", {0.1, 0.2, 0.3}))
          .AddVectorProperty("embedding2",
                             CreateVector("my_model", {-0.1, -0.2, -0.3}))
          .AddVectorProperty("embedding3",
                             CreateVector("my_model", {1.0, 2.0, 3.0}))
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document0).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  *search_spec.add_embedding_query_vectors() =
      CreateVector("my_model", {1, 1, 1});

  // Add a property filter for embedding1 and embedding2.
  TypePropertyMask* email_property_filters =
      search_spec.add_type_property_filters();
  email_property_filters->set_schema_type("Email");
  email_property_filters->add_paths("embedding1");
  email_property_filters->add_paths("embedding2");

  // Only the documents that have embedding1 or embedding2 will be returned.
  search_spec.set_query("semanticSearch(getEmbeddingParameter(0))");
  SearchResultProto results =
      icing.Search(search_spec, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(3));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document3));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
  EXPECT_THAT(results.results(2).document(), EqualsProto(document0));

  // Test that the property filters in SearchSpecProto can work together with
  // the property filter syntax in the query. Since they both filter on
  // embedding1, only the documents that have embedding1 will be returned.
  search_spec.set_query("embedding1:semanticSearch(getEmbeddingParameter(0))");
  results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document3));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document0));

  // No documents will be returned since the property filters in SearchSpecProto
  // and the property filters in the query are different.
  search_spec.set_query("embedding3:semanticSearch(getEmbeddingParameter(0))");
  results = icing.Search(search_spec, ScoringSpecProto::default_instance(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), IsEmpty());
}

TEST_F(IcingSearchEngineSearchTest, SearchWithUriFilters) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Add three documents with different URIs, namespaces, and "body" values.
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());
  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace2", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());
  DocumentProto document_three =
      DocumentBuilder()
          .SetKey("namespace2", "uri3")
          .SetSchema("Message")
          .AddStringProperty("body", "bar")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_three).status(), ProtoIsOk());

  // Create a search spec with an empty query and uri filters that should match
  // document1 and document3.
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");
  NamespaceDocumentUriGroup* namespace1_uris =
      search_spec.add_document_uri_filters();
  namespace1_uris->set_namespace_("namespace1");
  namespace1_uris->add_document_uris("uri1");
  NamespaceDocumentUriGroup* namespace2_uris =
      search_spec.add_document_uri_filters();
  namespace2_uris->set_namespace_("namespace2");
  namespace2_uris->add_document_uris("uri3");

  // Check results
  SearchResultProto results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_three));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));

  // Now set the query to "foo" and check that only document_one is returned.
  search_spec.set_query("foo");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_one));
}

TEST_F(IcingSearchEngineSearchTest,
       NamespacesInUriFiltersMustAppearInNamespaceFilters) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Add three documents with different URIs and namespaces.
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());
  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace2", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());
  DocumentProto document_three =
      DocumentBuilder()
          .SetKey("namespace2", "uri3")
          .SetSchema("Message")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_three).status(), ProtoIsOk());

  // Create a search spec with uri filters that include a namespace that is not
  // in the namespace filters.
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("foo");
  NamespaceDocumentUriGroup* namespace1_uris =
      search_spec.add_document_uri_filters();
  namespace1_uris->set_namespace_("namespace1");
  namespace1_uris->add_document_uris("uri1");
  NamespaceDocumentUriGroup* namespace2_uris =
      search_spec.add_document_uri_filters();
  namespace2_uris->set_namespace_("namespace2");
  namespace2_uris->add_document_uris("uri3");
  search_spec.add_namespace_filters("namespace1");

  // Check results
  SearchResultProto results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status().message(),
              HasSubstr("does not appear in the namespace filter"));
}

TEST_F(IcingSearchEngineSearchTest, UriFiltersWorkWithPropertyFilters) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());

  // Add three documents with different URIs.
  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Email")
          .AddStringProperty("subject", "foo")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status(), ProtoIsOk());
  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Email")
          .AddStringProperty("subject", "foo")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status(), ProtoIsOk());
  DocumentProto document_three =
      DocumentBuilder()
          .SetKey("namespace", "uri3")
          .SetSchema("Email")
          .AddStringProperty("subject", "baz")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_three).status(), ProtoIsOk());

  // Create a search spec with uri filters that should only match document1 and
  // document3, and a property filter that only searches for "subject". As a
  // result, only document1 should be returned.
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("foo");
  NamespaceDocumentUriGroup* uris = search_spec.add_document_uri_filters();
  uris->set_namespace_("namespace");
  uris->add_document_uris("uri1");
  uris->add_document_uris("uri3");
  TypePropertyMask* property_filters = search_spec.add_type_property_filters();
  property_filters->set_schema_type("Email");
  property_filters->add_paths("subject");

  // Check results
  SearchResultProto results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  EXPECT_THAT(results.results(), SizeIs(1));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_one));
}

TEST_F(IcingSearchEngineSearchTest, SearchWithRankingByScorableProperty) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "person0")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .AddDoubleProperty("income", 10000, 20000)
                                .Build();
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "person1")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "Johnson")
                                .AddDoubleProperty("income", 5000, 30000)
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("icing", "person2")
                                .SetSchema("Person")
                                .SetScore(20)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "Jack")
                                .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document0).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document2).status(), ProtoIsOk());

  // "J" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("J");

  // Ranking by documentScore and income.
  ScoringSpecProto scoring_spec_by_income = GetDefaultScoringSpec();
  scoring_spec_by_income.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec_by_income.set_advanced_scoring_expression(
      "this.documentScore() + sum(getScorableProperty(\"Person\", "
      "\"income\"))");
  scoring_spec_by_income.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);
  AddSchemaTypeAliasMap(&scoring_spec_by_income, "Person", {"Person"});

  int expected_person0_score = 10 + 10000 + 20000;
  int expected_person1_score = 10 + 5000 + 30000;
  int expected_person2_score = 20 + 0;

  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec_by_income, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  // Verify that the search results are ranked as expected.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("person1", "person0", "person2"));
  EXPECT_THAT(GetScoresFromSearchResults(search_result_proto),
              ElementsAre(expected_person1_score, expected_person0_score,
                          expected_person2_score));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithRankingByScorablePropertyWithFeatureDisabled) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  SearchSpecProto search_spec;

  // Ranking by documentScore and income.
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() + sum(getScorableProperty(\"Person\", "
      "\"income\"))");
  AddSchemaTypeAliasMap(&scoring_spec, "Person", {"Person"});

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "SCORABLE_PROPERTY_RANKING feature is not enabled.");

  SearchResultProto actual_results = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithRankingByScorableProperty_WithNestedSchemas) {
  SchemaProto schema_proto =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("id")
                                   .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                   .SetCardinality(CARDINALITY_REPEATED))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("age")
                          .SetDataType(PropertyConfigProto::DataType::INT64)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("isStarred")
                          .SetDataType(PropertyConfigProto::DataType::BOOLEAN)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("sender")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("receiver")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("scoreInt64")
                                        .SetScorableType(SCORABLE_TYPE_ENABLED)
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "email_id0")
          .SetSchema("Email")
          .SetScore(5)
          .AddInt64Property("scoreInt64", 1, 2, 3)
          .AddDocumentProperty("receiver",
                               DocumentBuilder()
                                   .SetKey("namespace", "receiver0")
                                   .SetSchema("Person")
                                   .AddInt64Property("age", 30)
                                   .AddDoubleProperty("income", 10, 20)
                                   .AddBooleanProperty("isStarred", true)
                                   .Build(),
                               DocumentBuilder()
                                   .SetKey("namespace", "receiver1")
                                   .SetSchema("Person")
                                   .AddInt64Property("age", 35)
                                   .AddDoubleProperty("income", 11, 12)
                                   .AddBooleanProperty("isStarred", false)
                                   .Build())
          .AddDocumentProperty("sender",
                               DocumentBuilder()
                                   .SetKey("namespace", "sender0")
                                   .SetSchema("Person")
                                   .AddInt64Property("age", 50)
                                   .AddDoubleProperty("income", 21, 22)
                                   .AddBooleanProperty("isStarred", false)
                                   .Build())
          .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema_proto).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() + "
      "sum(getScorableProperty(\"Email\", \"scoreInt64\")) + "
      "sum(getScorableProperty(\"Email\", \"sender.income\")) + "
      "5 * sum(getScorableProperty(\"Email\", \"receiver.income\"))");
  AddSchemaTypeAliasMap(&scoring_spec, "Person", {"Person"});
  AddSchemaTypeAliasMap(&scoring_spec, "Email", {"Email"});
  scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);
  int expected_score = 5 + (1 + 2 + 3) + (21 + 22) + 5 * (10 + 20 + 11 + 12);

  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("email_id0"));
  EXPECT_THAT(GetScoresFromSearchResults(search_result_proto),
              ElementsAre(expected_score));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithRankingByScorableProperty_WithInvalidPropertyName) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "person0")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .AddDoubleProperty("income", 10000, 20000)
                                .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document0).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() + sum(getScorableProperty(\"Person\", "
      "\"not_exist\"))");
  AddSchemaTypeAliasMap(&scoring_spec, "Person", {"Person"});
  scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "'not_exist' is not defined as a scorable property under schema type 0");

  SearchResultProto actual_search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(
      actual_search_result_proto,
      EqualsSearchResultIgnoreStatsAndScores(expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithRankingByScorableProperty_ScorablePropertyDisabled) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "person0")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .AddDoubleProperty("income", 10000, 20000)
                                .Build();
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_scorable_properties(false);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document0).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() + sum(getScorableProperty(\"Person\", "
      "\"not_exist\"))");
  AddSchemaTypeAliasMap(&scoring_spec, "Person", {"Person"});

  SearchResultProto results = icing.Search(search_spec, scoring_spec,
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest, JoinSearchWithRankingByScorableProperty) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("customizedScore")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("isStarred")
                          .SetDataType(PropertyConfigProto::DataType::BOOLEAN)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("CallLogSignalDoc")
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("personQualifiedId")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                   .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("rfsScore")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("SmsLogSignalDoc")
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("personQualifiedId")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                   .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("rfsScore")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // John has 2 call logs and 1 sms log to join.
  DocumentProto person_john_doc =
      DocumentBuilder()
          .SetKey("namespace", "person_john_id")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("Person")
          .SetScore(10)
          .AddBooleanProperty("isStarred", true)
          .AddDoubleProperty("customizedScore", 30)
          .AddStringProperty("name", "John")
          .Build();
  // Kevin has 2 call logs and 1 sms log to join.
  DocumentProto person_kevin_doc =
      DocumentBuilder()
          .SetKey("namespace", "person_kevin_id")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("Person")
          .SetScore(20)
          .AddBooleanProperty("isStarred", false)
          .AddDoubleProperty("customizedScore", 40)
          .AddStringProperty("name", "Kevin")
          .Build();
  // Tim has no signal docs to join.
  DocumentProto person_tim_doc =
      DocumentBuilder()
          .SetKey("namespace", "person_tim_id")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("Person")
          .SetScore(50)
          .AddDoubleProperty("customizedScore", 60)
          .AddStringProperty("name", "Tim")
          .Build();
  DocumentProto john_call_log_doc1 =
      DocumentBuilder()
          .SetKey("namespace", "call_log_id1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("CallLogSignalDoc")
          .SetScore(5)
          .AddDoubleProperty("rfsScore", 100, 200)
          .AddStringProperty("personQualifiedId", "namespace#person_john_id")
          .Build();
  DocumentProto john_call_log_doc2 =
      DocumentBuilder()
          .SetKey("namespace", "call_log_id2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("CallLogSignalDoc")
          .SetScore(5)
          .AddDoubleProperty("rfsScore", 200, 300)
          .AddStringProperty("personQualifiedId", "namespace#person_john_id")
          .Build();
  DocumentProto kevin_call_log_doc1 =
      DocumentBuilder()
          .SetKey("namespace", "call_log_id3")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("CallLogSignalDoc")
          .SetScore(5)
          .AddDoubleProperty("rfsScore", 300, 400)
          .AddStringProperty("personQualifiedId", "namespace#person_kevin_id")
          .Build();
  DocumentProto kevin_call_log_doc2 =
      DocumentBuilder()
          .SetKey("namespace", "call_log_id4")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("CallLogSignalDoc")
          .SetScore(5)
          .AddDoubleProperty("rfsScore", 500, 800)
          .AddStringProperty("personQualifiedId", "namespace#person_kevin_id")
          .Build();
  DocumentProto john_sms_log_doc1 =
      DocumentBuilder()
          .SetKey("namespace", "sms_log_id1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("SmsLogSignalDoc")
          .SetScore(5)
          .AddDoubleProperty("rfsScore", 1000, 2000)
          .AddStringProperty("personQualifiedId", "namespace#person_john_id")
          .Build();
  DocumentProto kevin_sms_log_doc1 =
      DocumentBuilder()
          .SetKey("namespace", "sms_log_id2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("SmsLogSignalDoc")
          .SetScore(5)
          .AddDoubleProperty("rfsScore", 2000, 3000)
          .AddStringProperty("personQualifiedId", "namespace#person_kevin_id")
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(person_john_doc).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(person_kevin_doc).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(person_tim_doc).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(john_call_log_doc1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(john_call_log_doc2).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(kevin_call_log_doc1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(kevin_call_log_doc2).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(john_sms_log_doc1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(kevin_sms_log_doc1).status(), ProtoIsOk());

  // all people will be matched.
  SearchSpecProto parent_search_spec;
  parent_search_spec.add_schema_type_filters("Person");

  // Child scoring spec: ranking by call logs and sms logs' rfsScore.
  ScoringSpecProto child_scoring_spec = GetDefaultScoringSpec();
  child_scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  child_scoring_spec.set_advanced_scoring_expression(
      "sum(getScorableProperty(\"CallLogSignalDoc\", \"rfsScore\")) + "
      "sum(getScorableProperty(\"SmsLogSignalDoc\", \"rfsScore\"))");
  AddSchemaTypeAliasMap(&child_scoring_spec, "Person", {"Person"});
  AddSchemaTypeAliasMap(&child_scoring_spec, "CallLogSignalDoc",
                        {"CallLogSignalDoc"});
  AddSchemaTypeAliasMap(&child_scoring_spec, "SmsLogSignalDoc",
                        {"SmsLogSignalDoc"});
  child_scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);

  JoinSpecProto* join_spec = parent_search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  *nested_spec->mutable_scoring_spec() = child_scoring_spec;
  nested_spec->mutable_search_spec()->add_schema_type_filters(
      "CallLogSignalDoc");
  nested_spec->mutable_search_spec()->add_schema_type_filters(
      "SmsLogSignalDoc");

  // Parent ScoringSpec
  ScoringSpecProto parent_scoring_spec = GetDefaultScoringSpec();
  parent_scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  parent_scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() + "
      "sum(getScorableProperty(\"Person\", \"customizedScore\")) + "
      "20 * sum(getScorableProperty(\"Person\", \"isStarred\")) + "
      "maxOrDefault(this.childrenRankingSignals(), 0)");
  AddSchemaTypeAliasMap(&parent_scoring_spec, "Person", {"Person"});
  AddSchemaTypeAliasMap(&parent_scoring_spec, "CallLogSignalDoc",
                        {"CallLogSignalDoc"});
  AddSchemaTypeAliasMap(&parent_scoring_spec, "SmsLogSignalDoc",
                        {"SmsLogSignalDoc"});
  parent_scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);
  int person_john_expected_score =
      10 + 30 + 20 * /*isStarred=*/1 +
      std::max({100 + 200, 300 + 400, 1000 + 2000});
  int person_kevin_expected_score =
      20 + 40 + 20 * /*isStarred=*/0 +
      std::max({300 + 400, 500 + 800, 2000 + 3000});
  int person_tim_expected_score =
      50 + 60 + 20 * /*isStarred=*/0 + /*child_ranking_signal=*/0;

  SearchResultProto search_result_proto =
      icing.Search(parent_search_spec, parent_scoring_spec,
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  // Verify that the search results are ranked as expected.
  EXPECT_THAT(
      GetUrisFromSearchResults(search_result_proto),
      ElementsAre("person_kevin_id", "person_john_id", "person_tim_id"));
  EXPECT_THAT(
      GetScoresFromSearchResults(search_result_proto),
      ElementsAre(person_kevin_expected_score, person_john_expected_score,
                  person_tim_expected_score));
}

TEST_F(IcingSearchEngineSearchTest,
       JoinSearchWithRankingByScorableProperty_ScorablePropertyDisabled) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("CallLogSignalDoc")
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("personQualifiedId")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                   .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("rfsScore")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // John has 2 call logs and 1 sms log to join.
  DocumentProto person_john_doc =
      DocumentBuilder()
          .SetKey("namespace", "person_john_id")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("Person")
          .SetScore(10)
          .AddStringProperty("name", "John")
          .Build();
  DocumentProto john_call_log_doc1 =
      DocumentBuilder()
          .SetKey("namespace", "call_log_id1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .SetSchema("CallLogSignalDoc")
          .SetScore(5)
          .AddDoubleProperty("rfsScore", 100, 200)
          .AddStringProperty("personQualifiedId", "namespace#person_john_id")
          .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_scorable_properties(false);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(person_john_doc).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(john_call_log_doc1).status(), ProtoIsOk());

  // all people will be matched.
  SearchSpecProto parent_search_spec;
  parent_search_spec.add_schema_type_filters("Person");

  // Child scoring spec: ranking by call logs and sms logs' rfsScore.
  ScoringSpecProto child_scoring_spec = GetDefaultScoringSpec();
  child_scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  child_scoring_spec.set_advanced_scoring_expression(
      "sum(getScorableProperty(\"CallLogSignalDoc\", \"rfsScore\"))");
  AddSchemaTypeAliasMap(&child_scoring_spec, "Person", {"Person"});
  AddSchemaTypeAliasMap(&child_scoring_spec, "CallLogSignalDoc",
                        {"CallLogSignalDoc"});

  JoinSpecProto* join_spec = parent_search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  *nested_spec->mutable_scoring_spec() = child_scoring_spec;
  nested_spec->mutable_search_spec()->add_schema_type_filters(
      "CallLogSignalDoc");

  // Parent ScoringSpec
  ScoringSpecProto parent_scoring_spec = GetDefaultScoringSpec();
  parent_scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  parent_scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() + "
      "maxOrDefault(this.childrenRankingSignals(), 0)");
  AddSchemaTypeAliasMap(&parent_scoring_spec, "Person", {"Person"});
  AddSchemaTypeAliasMap(&parent_scoring_spec, "CallLogSignalDoc",
                        {"CallLogSignalDoc"});

  SearchResultProto results =
      icing.Search(parent_search_spec, parent_scoring_spec,
                   ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithScorableProperty_DocumentsFromMultipleMatchedSchemaTypes) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("pkg1/db1/person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("pkg2/db2/person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "person0")
                                .SetSchema("pkg1/db1/person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddDoubleProperty("income", 10000, 20000)
                                .Build();
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "person1")
                                .SetSchema("pkg2/db2/person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddDoubleProperty("income", 30000, 40000)
                                .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document0).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());

  SearchSpecProto search_spec;

  // Ranking by income.
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "sum(getScorableProperty(\"Person\", \"income\"))");
  AddSchemaTypeAliasMap(&scoring_spec, "Person",
                        {"pkg1/db1/person", "pkg2/db2/person"});
  scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);
  int expected_person0_score = 10000 + 20000;
  int expected_person1_score = 30000 + 40000;

  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  // Verify that the search results are ranked as expected.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("person1", "person0"));
  EXPECT_THAT(GetScoresFromSearchResults(search_result_proto),
              ElementsAre(expected_person1_score, expected_person0_score));
}

TEST_F(IcingSearchEngineSearchTest,
       InvalidScoringSpecAliasMapWithEmptyAliasSchemaType) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  AddSchemaTypeAliasMap(&scoring_spec, /*alias_schema_type=*/"", {"Person"});

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "SchemaTypeAliasMapProto contains alias_schema_type with empty string");
  EXPECT_THAT(
      icing.Search(search_spec, scoring_spec,
                   ResultSpecProto::default_instance()),
      EqualsSearchResultIgnoreStatsAndScores(expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       InvalidScoringSpecAliasMapWithEmptySchemaTypes) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  AddSchemaTypeAliasMap(&scoring_spec, /*alias_schema_type=*/"Person", {});

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "SchemaTypeAliasMapProto contains empty schema_types for "
      "alias_schema_type: Person");
  EXPECT_THAT(
      icing.Search(search_spec, scoring_spec,
                   ResultSpecProto::default_instance()),
      EqualsSearchResultIgnoreStatsAndScores(expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       InvalidScoringSpecAliasMapWithDuplicatedAliasSchemaTypes) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  AddSchemaTypeAliasMap(&scoring_spec, /*alias_schema_type=*/"aliasPerson",
                        {"PersonA"});
  AddSchemaTypeAliasMap(&scoring_spec, /*alias_schema_type=*/"aliasPerson",
                        {"PersonB"});

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "SchemaTypeAliasMapProto contains multiple entries with the same "
      "alias_schema_type: aliasPerson");
  EXPECT_THAT(
      icing.Search(search_spec, scoring_spec,
                   ResultSpecProto::default_instance()),
      EqualsSearchResultIgnoreStatsAndScores(expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest, InvalidScoringSpecAliasMapFromJoinedQuery) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  SearchSpecProto parent_search_spec;
  ScoringSpecProto parent_scoring_spec = GetDefaultScoringSpec();
  AddSchemaTypeAliasMap(&parent_scoring_spec,
                        /*alias_schema_type=*/"aliasPerson", {"PersonA"});

  ScoringSpecProto child_scoring_spec = GetDefaultScoringSpec();
  AddSchemaTypeAliasMap(&child_scoring_spec, /*alias_schema_type=*/"",
                        {"PersonB"});

  JoinSpecProto* join_spec = parent_search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("personQualifiedId");
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  *nested_spec->mutable_scoring_spec() = child_scoring_spec;

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "SchemaTypeAliasMapProto contains alias_schema_type with empty string");
  EXPECT_THAT(
      icing.Search(parent_search_spec, parent_scoring_spec,
                   ResultSpecProto::default_instance()),
      EqualsSearchResultIgnoreStatsAndScores(expected_search_result_proto));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithScorablePropertyWithUpdatePropertyAsScorable) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "person0")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .AddDoubleProperty("income", 20000)
                                .Build();
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "person1")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .AddDoubleProperty("income", 10000)
                                .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document0).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);

  AddSchemaTypeAliasMap(&scoring_spec, "Person", {"Person"});
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "sum(getScorableProperty(\"Person\", \"income\"))");

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "'income' is not defined as a scorable property under schema type 0");

  SearchResultProto actual_search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(
      actual_search_result_proto,
      EqualsSearchResultIgnoreStatsAndScores(expected_search_result_proto));

  // Update the schema to set Person.income as a scorable property.
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  EXPECT_THAT(icing.SetSchema(new_schema).status(), ProtoIsOk());

  actual_search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(actual_search_result_proto.status(), ProtoIsOk());

  // Check the search results.
  EXPECT_THAT(GetUrisFromSearchResults(actual_search_result_proto),
              ElementsAre("person0", "person1"));
  EXPECT_THAT(GetScoresFromSearchResults(actual_search_result_proto),
              ElementsAre(20000, 10000));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithScorablePropertyWithFlipPropertyScorability) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "person0")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .AddDoubleProperty("income", 20000)
                                .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document0).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);

  AddSchemaTypeAliasMap(&scoring_spec, "Person", {"Person"});
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "sum(getScorableProperty(\"Person\", \"income\"))");

  // Update the schema to set Person.income as not scorable. It would erase the
  // data the scorable property cache.
  schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_DISABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "'income' is not defined as a scorable property under schema type 0");
  SearchResultProto actual_search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(
      actual_search_result_proto,
      EqualsSearchResultIgnoreStatsAndScores(expected_search_result_proto));

  // Update the schema to set Person.income as scorable again. It would
  // re-populate the scorable property cache.
  schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  // Check the search results.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("person0"));
  EXPECT_THAT(GetScoresFromSearchResults(search_result_proto),
              ElementsAre(20000));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithScorablePropertyWithAddNewPropertyAsScorable) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "person0")
                                .SetSchema("Person")
                                .SetScore(5)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .Build();
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "person1")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document0).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());

  // Update the schema to add Person.income as a scorable property.
  // It would populate the scorable property cache with the default values.
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  EXPECT_THAT(icing.SetSchema(new_schema).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);

  AddSchemaTypeAliasMap(&scoring_spec, "Person", {"Person"});
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() +sum(getScorableProperty(\"Person\", \"income\"))");

  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  // Check the search results.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("person1", "person0"));
  EXPECT_THAT(GetScoresFromSearchResults(search_result_proto),
              ElementsAre(10, 5));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithScorablePropertyWithReorderScorableProperties) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("age")
                          .SetDataType(PropertyConfigProto::DataType::INT64)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "person0")
                                .SetSchema("Person")
                                .SetScore(5)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "John")
                                .AddInt64Property("age", 20)
                                .AddDoubleProperty("income", 10000, 20000)
                                .Build();
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "person1")
                                .SetSchema("Person")
                                .SetScore(10)
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("name", "Kevin")
                                .AddInt64Property("age", 25)
                                .AddDoubleProperty("income", 30000, 40000)
                                .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document0).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());

  // Update the schema change the order of the properties "age" and "income".
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("age")
                          .SetDataType(PropertyConfigProto::DataType::INT64)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  EXPECT_THAT(icing.SetSchema(new_schema).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);

  AddSchemaTypeAliasMap(&scoring_spec, "Person", {"Person"});
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "this.documentScore() + sum(getScorableProperty(\"Person\", \"income\")) "
      "+ sum(getScorableProperty(\"Person\", \"age\"))");
  int expected_score0 = 5 + (10000 + 20000) + 20;
  int expected_score1 = 10 + (30000 + 40000) + 25;

  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  // Check the search results.
  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("person1", "person0"));
  EXPECT_THAT(GetScoresFromSearchResults(search_result_proto),
              ElementsAre(expected_score1, expected_score0));
}

TEST_F(IcingSearchEngineSearchTest,
       SearchWithScorablePropertyWithUpdatePropertyAsScorableInNestedSchema) {
  SchemaProto schema_proto =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("id")
                                   .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                   .SetCardinality(CARDINALITY_REPEATED))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_DISABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("sender")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("receiver")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "email_id0")
          .SetSchema("Email")
          .AddDocumentProperty("receiver",
                               DocumentBuilder()
                                   .SetKey("namespace", "receiver0")
                                   .SetSchema("Person")
                                   .AddDoubleProperty("income", 10, 20)
                                   .Build(),
                               DocumentBuilder()
                                   .SetKey("namespace", "receiver1")
                                   .SetSchema("Person")
                                   .AddDoubleProperty("income", 11, 12)
                                   .Build())
          .AddDocumentProperty("sender",
                               DocumentBuilder()
                                   .SetKey("namespace", "sender0")
                                   .SetSchema("Person")
                                   .AddDoubleProperty("income", 21, 22)
                                   .Build())
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema_proto).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Update the 'Person' schema by setting Person.income as scorable.
  // It would trigger the re-generation of the scorable property cache for the
  // the schema 'Email', as it is a parent schema of 'Person'.
  schema_proto =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Person")
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("id")
                                   .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                   .SetCardinality(CARDINALITY_REPEATED))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("income")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)
                          .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("sender")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("receiver")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  EXPECT_THAT(icing.SetSchema(schema_proto).status(), ProtoIsOk());

  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.add_scoring_feature_types_enabled(
      ScoringFeatureType::SCORABLE_PROPERTY_RANKING);

  AddSchemaTypeAliasMap(&scoring_spec, "Email", {"Email"});
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(
      "sum(getScorableProperty(\"Email\", \"sender.income\")) + "
      "sum(getScorableProperty(\"Email\", \"receiver.income\"))");
  int expected_score = (21 + 22) + (10 + 20 + 11 + 12);

  SearchResultProto search_result_proto = icing.Search(
      search_spec, scoring_spec, ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto.status(), ProtoIsOk());

  EXPECT_THAT(GetUrisFromSearchResults(search_result_proto),
              ElementsAre("email_id0"));
  EXPECT_THAT(GetScoresFromSearchResults(search_result_proto),
              ElementsAre(expected_score));
}

}  // namespace
}  // namespace lib
}  // namespace icing
