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

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "testing/base/public/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/document-generator.h"
#include "icing/testing/random-string.h"
#include "icing/testing/recorder-test-utils.h"
#include "icing/testing/schema-generator.h"
#include "icing/testing/tmp-directory.h"

// Run on a Linux workstation:
//    $ blaze build -c opt --dynamic_mode=off --copt=-gmlt
//    //icing:icing-search-engine_benchmark
//
//    $ blaze-bin/icing/icing-search-engine_benchmark
//    --benchmarks=all --benchmark_memory_usage
//
// Run on an Android device:
//    $ blaze build --copt="-DGOOGLE_COMMANDLINEFLAGS_FULL_API=1"
//    --config=android_arm64 -c opt --dynamic_mode=off --copt=-gmlt
//    //icing:icing-search-engine_benchmark
//
//    $ adb push blaze-bin/icing/icing-search-engine_benchmark
//    /data/local/tmp/
//
//    $ adb shell /data/local/tmp/icing-search-engine_benchmark --benchmarks=all

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;

// Icing GMSCore has, on average, 17 corpora on a device and 30 corpora at the
// 95th pct. Most clients use a single type. This is a function of Icing's
// constrained type offering. Assume that each package will use 3 types on
// average.
constexpr int kAvgNumNamespaces = 10;
constexpr int kAvgNumTypes = 3;

// ASSUME: Properties will have at most ten properties. Types will be created
// with [1, 10] properties.
constexpr int kMaxNumProperties = 10;

// Based on logs from Icing GMSCore.
constexpr int kAvgDocumentSize = 300;

// ASSUME: ~75% of the document's size comes from it's content.
constexpr float kContentSizePct = 0.7;

// Average length of word in English is 4.7 characters.
constexpr int kAvgTokenLen = 5;
// Made up value. This results in a fairly reasonable language - the majority of
// generated words are 3-9 characters, ~3% of words are >=20 chars, and the
// longest ones are 27 chars, (roughly consistent with the longest,
// non-contrived English words
// https://en.wikipedia.org/wiki/Longest_word_in_English)
constexpr int kTokenStdDev = 7;
constexpr int kLanguageSize = 1000;

// Lite Index size required to fit 128k docs, each doc requires ~64 bytes of
// space in the lite index.
constexpr int kIcingFullIndexSize = 1024 * 1024 * 8;

// Query params
constexpr int kNumPerPage = 10;
constexpr int kNumToSnippet = 10000;
constexpr int kMatchesPerProperty = 1;

std::vector<std::string> CreateNamespaces(int num_namespaces) {
  std::vector<std::string> namespaces;
  while (--num_namespaces >= 0) {
    namespaces.push_back("comgooglepackage" + std::to_string(num_namespaces));
  }
  return namespaces;
}

// Creates a vector containing num_words randomly-generated words for use by
// documents.
template <typename Rand>
std::vector<std::string> CreateLanguage(int num_words, Rand* r) {
  std::vector<std::string> language;
  std::normal_distribution<> norm_dist(kAvgTokenLen, kTokenStdDev);
  while (--num_words >= 0) {
    int word_length = 0;
    while (word_length < 1) {
      word_length = std::round(norm_dist(*r));
    }
    language.push_back(RandomString(kAlNumAlphabet, word_length, r));
  }
  return language;
}

SearchSpecProto CreateSearchSpec(const std::string& query,
                                 const std::vector<std::string>& namespaces,
                                 TermMatchType::Code match_type) {
  SearchSpecProto search_spec;
  search_spec.set_query(query);
  for (const std::string& name_space : namespaces) {
    search_spec.add_namespace_filters(name_space);
  }
  search_spec.set_term_match_type(match_type);
  return search_spec;
}

ResultSpecProto CreateResultSpec(int num_per_page, int num_to_snippet,
                                 int matches_per_property) {
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(num_per_page);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(num_to_snippet);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(
      matches_per_property);
  return result_spec;
}

ScoringSpecProto CreateScoringSpec(
    ScoringSpecProto::RankingStrategy::Code ranking_strategy) {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ranking_strategy);
  return scoring_spec;
}

class DestructibleDirectory {
 public:
  explicit DestructibleDirectory(const Filesystem& filesystem,
                                 const std::string& dir)
      : filesystem_(filesystem), dir_(dir) {
    filesystem_.CreateDirectoryRecursively(dir_.c_str());
  }
  ~DestructibleDirectory() {
    filesystem_.DeleteDirectoryRecursively(dir_.c_str());
  }

 private:
  Filesystem filesystem_;
  std::string dir_;
};

void BM_MutlipleIndices(benchmark::State& state) {
  // Initialize the filesystem
  std::string test_dir = GetTestTempDir() + "/icing/benchmark";
  Filesystem filesystem;
  DestructibleDirectory ddir(filesystem, test_dir);

  // Create the schema.
  std::default_random_engine random;
  int num_types = kAvgNumNamespaces * kAvgNumTypes;
  ExactStringPropertyGenerator property_generator;
  RandomSchemaGenerator<std::default_random_engine,
                        ExactStringPropertyGenerator>
      schema_generator(&random, &property_generator);
  SchemaProto schema =
      schema_generator.GenerateSchema(num_types, kMaxNumProperties);
  EvenDistributionTypeSelector type_selector(schema);

  // Create the indices.
  std::vector<std::unique_ptr<IcingSearchEngine>> icings;
  int num_indices = state.range(0);
  for (int i = 0; i < num_indices; ++i) {
    IcingSearchEngineOptions options;
    std::string base_dir = test_dir + "/" + std::to_string(i);
    options.set_base_dir(base_dir);
    options.set_index_merge_size(kIcingFullIndexSize / num_indices);
    auto icing = std::make_unique<IcingSearchEngine>(options);

    InitializeResultProto init_result = icing->Initialize();
    ASSERT_THAT(init_result.status().code(), Eq(StatusProto::OK));

    SetSchemaResultProto schema_result = icing->SetSchema(schema);
    ASSERT_THAT(schema_result.status().code(), Eq(StatusProto::OK));
    icings.push_back(std::move(icing));
  }

  // Setup namespace info and language
  std::vector<std::string> namespaces = CreateNamespaces(kAvgNumNamespaces);
  EvenDistributionNamespaceSelector namespace_selector(namespaces);

  std::vector<std::string> language = CreateLanguage(kLanguageSize, &random);
  UniformDistributionLanguageTokenGenerator<std::default_random_engine>
      token_generator(language, &random);

  // Fill the index.
  DocumentGenerator<
      EvenDistributionNamespaceSelector, EvenDistributionTypeSelector,
      UniformDistributionLanguageTokenGenerator<std::default_random_engine>>
      generator(&namespace_selector, &type_selector, &token_generator,
                kAvgDocumentSize * kContentSizePct);
  for (int i = 0; i < state.range(1); ++i) {
    DocumentProto doc = generator.generateDoc();
    PutResultProto put_result;
    if (icings.empty()) {
      ASSERT_THAT(put_result.status().code(), Eq(StatusProto::UNKNOWN));
      continue;
    }
    put_result = icings.at(i % icings.size())->Put(doc);
    ASSERT_THAT(put_result.status().code(), Eq(StatusProto::OK));
  }

  // QUERY!
  // Every document has its own namespace as a token. This query that should
  // match 1/kAvgNumNamespace% of all documents.
  const std::string& name_space = namespaces.at(0);
  SearchSpecProto search_spec = CreateSearchSpec(
      /*query=*/name_space, {name_space}, TermMatchType::EXACT_ONLY);
  ResultSpecProto result_spec =
      CreateResultSpec(kNumPerPage, kNumToSnippet, kMatchesPerProperty);
  ScoringSpecProto scoring_spec =
      CreateScoringSpec(ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);

  int num_results = 0;
  for (auto _ : state) {
    num_results = 0;
    SearchResultProto result;
    if (icings.empty()) {
      ASSERT_THAT(result.status().code(), Eq(StatusProto::UNKNOWN));
      continue;
    }
    result = icings.at(0)->Search(search_spec, scoring_spec, result_spec);
    ASSERT_THAT(result.status().code(), Eq(StatusProto::OK));
    while (!result.results().empty()) {
      num_results += result.results_size();
      if (!icings.empty()) {
        result = icings.at(0)->GetNextPage(result.next_page_token());
      }
      ASSERT_THAT(result.status().code(), Eq(StatusProto::OK));
    }
  }

  // Measure size.
  int64_t disk_usage = filesystem.GetDiskUsage(test_dir.c_str());
  std::cout << "Num results:\t" << num_results << "\t\tDisk Use:\t"
            << disk_usage / 1024.0 << std::endl;
}
BENCHMARK(BM_MutlipleIndices)
    // First argument: num_indices, Second argument: num_total_documents
    // So each index will contain (num_total_documents / num_indices) documents.
    ->ArgPair(0, 0)
    ->ArgPair(0, 1024)
    ->ArgPair(0, 131072)
    ->ArgPair(1, 0)
    ->ArgPair(1, 1)
    ->ArgPair(1, 2)
    ->ArgPair(1, 8)
    ->ArgPair(1, 32)
    ->ArgPair(1, 128)
    ->ArgPair(1, 1024)
    ->ArgPair(1, 8192)
    ->ArgPair(1, 32768)
    ->ArgPair(1, 131072)
    ->ArgPair(2, 0)
    ->ArgPair(2, 1)
    ->ArgPair(2, 2)
    ->ArgPair(2, 8)
    ->ArgPair(2, 32)
    ->ArgPair(2, 128)
    ->ArgPair(2, 1024)
    ->ArgPair(2, 8192)
    ->ArgPair(2, 32768)
    ->ArgPair(2, 131072)
    ->ArgPair(10, 0)
    ->ArgPair(10, 1)
    ->ArgPair(10, 2)
    ->ArgPair(10, 8)
    ->ArgPair(10, 32)
    ->ArgPair(10, 128)
    ->ArgPair(10, 1024)
    ->ArgPair(10, 8192)
    ->ArgPair(10, 32768)
    ->ArgPair(10, 131072);

}  // namespace

}  // namespace lib
}  // namespace icing
