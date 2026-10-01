// Copyright (C) 2026 Google LLC
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

#include <atomic>
#include <chrono>  // NOLINT
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/jni/jni-cache.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/platform.h"
#include "icing/proto/ann.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/query/query-features.h"
#include "icing/schema-builder.h"
#include "icing/testing/blocking-clock.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/icu-data-file-helper.h"

namespace icing {
namespace lib {

namespace {

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

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_document_store_namespace_id_fingerprint(true);
  icing_options.set_enable_repeated_field_joins(true);
  icing_options.set_enable_delete_propagation_from(true);
  icing_options.set_enable_non_existent_qualified_id_join(true);
  return icing_options;
}

ScoringSpecProto GetDefaultScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

class IcingSearchEngineMaintainAnnIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing/maintain_ann_test";
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  void TearDown() override {
    icing_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  void SetUpEngine(bool enable_read_during_ann_maintenance) {
    IcingSearchEngineOptions options;
    options.set_base_dir(test_dir_);
    options.set_enable_read_during_ann_maintenance(
        enable_read_during_ann_maintenance);
    options.set_embedding_index_num_shards(1);

    auto blocking_clock = std::make_unique<BlockingClock>();
    blocking_clock_ptr_ = blocking_clock.get();

    icing_ = std::make_unique<TestIcingSearchEngine>(
        options, std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::move(blocking_clock),
        nullptr);
    ASSERT_THAT(icing_->Initialize().status(), ProtoIsOk());

    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Email")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("body")
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("vector")
                            .SetDataTypeVector(
                                EMBEDDING_INDEXING_APPROXIMATE_NEAREST_NEIGHBOR)
                            .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    ASSERT_THAT(icing_->SetSchema(schema).status(), ProtoIsOk());

    // Insert 3 documents to exceed min_size_for_ivf=2
    ASSERT_THAT(icing_
                    ->Put(DocumentBuilder()
                              .SetKey("ns", "email_1")
                              .SetSchema("Email")
                              .AddStringProperty("body", "hello")
                              .AddVectorProperty(
                                  "vector",
                                  CreateVector("my_model", {1.0f, 0.0f, 0.0f}))
                              .Build())
                    .status(),
                ProtoIsOk());
    ASSERT_THAT(icing_
                    ->Put(DocumentBuilder()
                              .SetKey("ns", "email_2")
                              .SetSchema("Email")
                              .AddStringProperty("body", "hello")
                              .AddVectorProperty(
                                  "vector",
                                  CreateVector("my_model", {0.0f, 1.0f, 0.0f}))
                              .Build())
                    .status(),
                ProtoIsOk());
    ASSERT_THAT(icing_
                    ->Put(DocumentBuilder()
                              .SetKey("ns", "email_3")
                              .SetSchema("Email")
                              .AddStringProperty("body", "hello")
                              .AddVectorProperty(
                                  "vector",
                                  CreateVector("my_model", {0.0f, 0.0f, 1.0f}))
                              .Build())
                    .status(),
                ProtoIsOk());
  }

  std::string test_dir_;
  Filesystem filesystem_;
  BlockingClock* blocking_clock_ptr_ = nullptr;
  std::unique_ptr<TestIcingSearchEngine> icing_;
};

TEST_F(IcingSearchEngineMaintainAnnIndexTest,
       MaintainAnnIndexUnblocksQueryWhenFlagEnabled) {
  SetUpEngine(/*enable_read_during_ann_maintenance=*/true);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(2);
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.mutable_mini_batch_k_means_options()->set_min_num_iterations(
      1);

  // Enable blocking right before calling MaintainAnnIndex
  blocking_clock_ptr_->EnableBlockOnTimerQuery();

  // Start MaintainAnnIndex in background thread
  std::thread maintain_thread([this, &maintain_options]() {
    icing_->MaintainAnnIndex(maintain_options);
  });

  // Wait until the background thread is blocked at the timer
  blocking_clock_ptr_->WaitUntilBlocked();

  // Perform a search query. It should succeed immediately without blocking!
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("hello");
  ScoringSpecProto scoring_spec;
  ResultSpecProto result_spec;

  SearchResultProto search_result =
      icing_->Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_result.status(), ProtoIsOk());
  EXPECT_THAT(search_result.results(), testing::SizeIs(3));

  // Unblock the background thread
  blocking_clock_ptr_->Unblock();
  maintain_thread.join();
}

TEST_F(IcingSearchEngineMaintainAnnIndexTest,
       MaintainAnnIndexBlocksQueryWhenFlagDisabled) {
  SetUpEngine(/*enable_read_during_ann_maintenance=*/false);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(2);
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.mutable_mini_batch_k_means_options()->set_min_num_iterations(
      1);

  // Enable blocking right before calling MaintainAnnIndex
  blocking_clock_ptr_->EnableBlockOnTimerQuery();

  // Start MaintainAnnIndex in background thread
  std::thread maintain_thread([this, &maintain_options]() {
    icing_->MaintainAnnIndex(maintain_options);
  });

  // Wait until the background thread is blocked at the timer
  blocking_clock_ptr_->WaitUntilBlocked();

  // Try to search query in another thread. It should block!
  std::atomic<bool> query_completed{false};
  std::thread query_thread([this, &query_completed]() {
    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec.set_query("hello");
    ScoringSpecProto scoring_spec;
    ResultSpecProto result_spec;
    SearchResultProto search_result =
        icing_->Search(search_spec, scoring_spec, result_spec);
    EXPECT_THAT(search_result.status(), ProtoIsOk());
    query_completed.store(true);
  });

  // Sleep for 2000ms, the query should NOT have completed because it is
  // blocked.
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  EXPECT_FALSE(query_completed.load());

  // Unblock the background thread
  blocking_clock_ptr_->Unblock();

  // Now the query should complete and maintain thread should join.
  query_thread.join();
  EXPECT_TRUE(query_completed.load());
  maintain_thread.join();
}

class IcingSearchEngineAnnConcurrencyTest : public ::testing::Test {
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

  void SetUpEngine() {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_read_during_ann_maintenance(true);
    options.set_embedding_index_num_shards(1);

    icing_ = std::make_unique<IcingSearchEngine>(options, GetTestJniCache());
    ASSERT_THAT(icing_->Initialize().status(), ProtoIsOk());

    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Email")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("body")
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("vector")
                            .SetDataTypeVector(
                                EMBEDDING_INDEXING_APPROXIMATE_NEAREST_NEIGHBOR)
                            .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    ASSERT_THAT(icing_->SetSchema(schema).status(), ProtoIsOk());
  }

  // Builds a document with a distinct, deterministic, non-zero embedding.
  static DocumentProto MakeDocument(const std::string& uri, int seed) {
    return DocumentBuilder()
        .SetKey("ns", uri)
        .SetSchema("Email")
        .AddStringProperty("body", "hello world")
        .AddVectorProperty(
            "vector",
            CreateVector("my_model", {static_cast<float>(seed % 7) + 1.0f,
                                      static_cast<float>(seed % 5) + 1.0f,
                                      static_cast<float>(seed % 3) + 1.0f}))
        .Build();
  }

  void SeedDocuments(int count) {
    for (int i = 0; i < count; ++i) {
      ASSERT_THAT(
          icing_->Put(MakeDocument("seed_" + std::to_string(i), i)).status(),
          ProtoIsOk());
    }
  }

  // Read-only semanticSearch spec whose score bounds are wide enough to match
  // every indexed embedding and whose nprobe is large enough to probe every IVF
  // cluster, so a full search returns exactly every document that has a vector.
  static SearchSpecProto MakeSemanticSearchSpec() {
    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec.set_use_read_only_search(true);
    search_spec.set_embedding_query_metric_type(
        SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
    search_spec.add_enabled_features(
        std::string(kListFilterQueryLanguageFeature));
    *search_spec.add_embedding_query_vectors() =
        CreateVector("my_model", {1.0f, 1.0f, 1.0f});
    search_spec.set_embedding_query_nprobe(1000);
    search_spec.set_query(
        "semanticSearch(getEmbeddingParameter(0), -1000000, 1000000, "
        "\"DOT_PRODUCT\")");
    return search_spec;
  }

  static MaintainAnnIndexOptions MakeMaintainOptions() {
    MaintainAnnIndexOptions options;
    options.set_min_size_for_ivf(2);
    options.set_rebuild_threshold(0.0);
    options.mutable_mini_batch_k_means_options()->set_target_cluster_size(4);
    options.mutable_mini_batch_k_means_options()->set_min_num_iterations(1);
    return options;
  }

  // Returns the number of documents matched by a full read-only semantic
  // search. Fails (via EXPECT) if the search itself does not succeed.
  int CountAllMatchingDocs() {
    ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
    ResultSpecProto result_spec = ResultSpecProto::default_instance();
    result_spec.set_num_per_page(100000);
    SearchResultProto results =
        icing_->Search(MakeSemanticSearchSpec(), scoring_spec, result_spec);
    EXPECT_THAT(results.status(), ProtoIsOk());
    return results.results_size();
  }

  Filesystem filesystem_;
  std::unique_ptr<IcingSearchEngine> icing_;
};

// Many concurrent read-only semantic searches running while a maintenance pass
// executes. Every search must succeed, and after everything settles every
// seeded document is still found.
TEST_F(IcingSearchEngineAnnConcurrencyTest,
       ConcurrentSemanticSearchDuringMaintain) {
  SetUpEngine();
  constexpr int kNumSeedDocs = 20;
  SeedDocuments(kNumSeedDocs);
  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());

  constexpr int kNumReaders = 20;
  constexpr int kNumMaintainers = 3;
  constexpr int kReaderIterations = 25;
  std::atomic<bool> encountered_error{false};

  auto reader_callable = [&]() {
    for (int i = 0; i < kReaderIterations; ++i) {
      if (CountAllMatchingDocs() < 0) {
        encountered_error.store(true);
      }
    }
  };
  auto maintainer_callable = [&]() {
    if (icing_->MaintainAnnIndex(MakeMaintainOptions()).status().code() !=
        StatusProto::OK) {
      encountered_error.store(true);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumReaders; ++i) {
    threads.emplace_back(reader_callable);
  }
  for (int i = 0; i < kNumMaintainers; ++i) {
    threads.emplace_back(maintainer_callable);
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_FALSE(encountered_error.load());
  // A final pass and full search must find every seeded document.
  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());
  EXPECT_EQ(CountAllMatchingDocs(), kNumSeedDocs);
}

// Concurrent Put (each adding a new document with an embedding) while a
// maintenance pass runs. Put takes an exclusive engine lock and maintenance a
// shared lock, so they serialize with no deadlock. After joining, every
// document (seed + newly written) must be present.
TEST_F(IcingSearchEngineAnnConcurrencyTest, ConcurrentPutDuringMaintain) {
  SetUpEngine();
  constexpr int kNumSeedDocs = 10;
  SeedDocuments(kNumSeedDocs);
  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());

  constexpr int kNumWriters = 15;
  std::vector<char> put_ok(kNumWriters, 0);
  std::atomic<bool> maintain_ok{true};

  std::thread maintain_thread([&]() {
    if (icing_->MaintainAnnIndex(MakeMaintainOptions()).status().code() !=
        StatusProto::OK) {
      maintain_ok.store(false);
    }
  });

  std::vector<std::thread> writer_threads;
  for (int i = 0; i < kNumWriters; ++i) {
    writer_threads.emplace_back([&, i]() {
      PutResultProto result = icing_->Put(
          MakeDocument("concurrent_" + std::to_string(i), 1000 + i));
      put_ok[i] = (result.status().code() == StatusProto::OK) ? 1 : 0;
    });
  }
  for (auto& thread : writer_threads) {
    thread.join();
  }
  maintain_thread.join();

  for (int i = 0; i < kNumWriters; ++i) {
    EXPECT_TRUE(put_ok[i]) << "Put failed on writer thread " << i;
  }
  EXPECT_TRUE(maintain_ok.load());

  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());
  EXPECT_EQ(CountAllMatchingDocs(), kNumSeedDocs + kNumWriters);
}

// The full gauntlet: readers (semantic search), writers (Put), and maintainers
// all colliding at once, repeated over many iterations. Verifies no deadlock or
// crash, and an exact final document count.
TEST_F(IcingSearchEngineAnnConcurrencyTest, MixedConcurrentReadWriteMaintain) {
  SetUpEngine();
  constexpr int kNumSeedDocs = 10;
  SeedDocuments(kNumSeedDocs);
  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());

  constexpr int kNumReaderThreads = 8;
  constexpr int kNumWriterThreads = 4;
  constexpr int kNumMaintainerThreads = 2;
  constexpr int kReaderIterations = 30;
  constexpr int kWriterIterations = 15;
  constexpr int kMaintainerIterations = 10;
  std::atomic<bool> encountered_error{false};

  auto reader_callable = [&]() {
    for (int i = 0; i < kReaderIterations; ++i) {
      if (CountAllMatchingDocs() < 0) {
        encountered_error.store(true);
      }
    }
  };
  auto writer_callable = [&](int writer_id) {
    for (int i = 0; i < kWriterIterations; ++i) {
      std::string uri =
          "w" + std::to_string(writer_id) + "_" + std::to_string(i);
      if (icing_->Put(MakeDocument(uri, writer_id * 100 + i)).status().code() !=
          StatusProto::OK) {
        encountered_error.store(true);
      }
    }
  };
  auto maintainer_callable = [&]() {
    for (int i = 0; i < kMaintainerIterations; ++i) {
      if (icing_->MaintainAnnIndex(MakeMaintainOptions()).status().code() !=
          StatusProto::OK) {
        encountered_error.store(true);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumReaderThreads; ++i) {
    threads.emplace_back(reader_callable);
  }
  for (int i = 0; i < kNumWriterThreads; ++i) {
    threads.emplace_back(writer_callable, i);
  }
  for (int i = 0; i < kNumMaintainerThreads; ++i) {
    threads.emplace_back(maintainer_callable);
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_FALSE(encountered_error.load())
      << "A concurrent read/write/maintenance operation failed.";
  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());
  EXPECT_EQ(CountAllMatchingDocs(),
            kNumSeedDocs + kNumWriterThreads * kWriterIterations);
}

// Multiple MaintainAnnIndex calls fired simultaneously. They must all return OK
// (overlapping calls simply return early via the internal maintenance guard),
// with no deadlock/crash, and the index must remain fully queryable afterward.
TEST_F(IcingSearchEngineAnnConcurrencyTest,
       MultipleConcurrentMaintainAnnIndex) {
  SetUpEngine();
  constexpr int kNumSeedDocs = 20;
  SeedDocuments(kNumSeedDocs);

  constexpr int kMaintainThreads = 8;
  std::vector<char> maintain_ok(kMaintainThreads, 0);
  std::vector<std::thread> threads;
  for (int i = 0; i < kMaintainThreads; ++i) {
    threads.emplace_back([&, i]() {
      maintain_ok[i] =
          (icing_->MaintainAnnIndex(MakeMaintainOptions()).status().code() ==
           StatusProto::OK)
              ? 1
              : 0;
    });
  }
  for (int i = 0; i < kMaintainThreads; ++i) {
    threads[i].join();
    EXPECT_TRUE(maintain_ok[i]) << "MaintainAnnIndex failed on thread " << i;
  }

  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());
  EXPECT_EQ(CountAllMatchingDocs(), kNumSeedDocs);
}

// Sustained activity across several rounds: each round runs a maintenance pass
// concurrently with fresh reads and writes. Verifies stability over repeated
// maintenance cycles and an exact final document count.
TEST_F(IcingSearchEngineAnnConcurrencyTest,
       RepeatedMaintainWithConcurrentActivity) {
  SetUpEngine();
  constexpr int kNumSeedDocs = 10;
  SeedDocuments(kNumSeedDocs);
  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());

  constexpr int kRounds = 3;
  constexpr int kPutsPerRound = 5;
  constexpr int kReadsPerRound = 5;
  std::atomic<bool> encountered_error{false};

  for (int round = 0; round < kRounds; ++round) {
    std::vector<std::thread> threads;
    threads.emplace_back([&]() {
      if (icing_->MaintainAnnIndex(MakeMaintainOptions()).status().code() !=
          StatusProto::OK) {
        encountered_error.store(true);
      }
    });
    for (int i = 0; i < kReadsPerRound; ++i) {
      threads.emplace_back([&]() {
        if (CountAllMatchingDocs() < 0) {
          encountered_error.store(true);
        }
      });
    }
    for (int i = 0; i < kPutsPerRound; ++i) {
      threads.emplace_back([&, i]() {
        std::string uri =
            "round_" + std::to_string(round) + "_" + std::to_string(i);
        if (icing_->Put(MakeDocument(uri, round * 1000 + i)).status().code() !=
            StatusProto::OK) {
          encountered_error.store(true);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }

  EXPECT_FALSE(encountered_error.load());
  ASSERT_THAT(icing_->MaintainAnnIndex(MakeMaintainOptions()).status(),
              ProtoIsOk());
  EXPECT_EQ(CountAllMatchingDocs(), kNumSeedDocs + kRounds * kPutsPerRound);
}

}  // namespace
}  // namespace lib
}  // namespace icing
