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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/embed/embedding-reference.h"
#include "icing/index/hit/hit.h"
#include "icing/portable/gzip_stream.h"
#include "icing/proto/ann.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/document-util.h"
#include "icing/util/embedding-util.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Ge;
using ::testing::IsTrue;

static constexpr uint32_t kDimension = 3;
// Use a single shard so all activity collides on the same storage and lock,
// maximizing contention.
static constexpr uint32_t kNumShards = 1;
static const char kModelSignature[] = "model";
static constexpr std::string_view kSchemaName = "type";

static constexpr int kNumThreads = 50;

}  // namespace

// Thread-safety tests for the EmbeddingIndex component. Inherits from
// EmbeddingIndexTestPeer to access the lock-holding static helpers
// (GetEmbeddingVector / AppendEmbeddingVector / GetShardId etc.).
class EmbeddingIndexThreadSafetyTest : public testing::Test,
                                       public EmbeddingIndexTestPeer {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    // Sanity check: the whole point of these tests is that the internal mutex
    // is a real reader/writer lock, which only happens when the flag is on.
    ASSERT_THAT(feature_flags_->enable_read_during_ann_maintenance(), IsTrue());

    test_dir_ = GetTestTempDir() + "/icing";
    embedding_index_dir_ = test_dir_ + "/embedding_index";
    document_store_dir_ = test_dir_ + "/document_store";
    schema_store_dir_ = test_dir_ + "/schema_store";
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(document_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &clock_, feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(
            &filesystem_, document_store_dir_, &clock_, schema_store_.get(),
            feature_flags_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/true,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionLevel,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionThresholdBytes,
            protobuf_ports::kDefaultMemLevel,
            /*initialize_stats=*/nullptr));
    document_store_ = std::move(create_result.document_store);

    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_, &clock_,
                               feature_flags_.get(), kNumShards));

    ICING_ASSERT_OK(schema_store_->SetSchema(
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType(kSchemaName)
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("prop1")
                            .SetDataTypeVector(
                                EMBEDDING_INDEXING_APPROXIMATE_NEAREST_NEIGHBOR)
                            .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build(),
        /*ignore_errors_and_delete_documents=*/false));

    default_shard_id_ = GetShardId(embedding_index_.get(), kDimension,
                                   kModelSignature, kSchemaName);
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    embedding_index_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  // Adds `num_docs` documents to the document store (uri0..uri{n-1}) so their
  // embeddings can be resolved during IVF maintenance.
  void PutDocuments(int num_docs) {
    for (int i = 0; i < num_docs; ++i) {
      ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
          DocumentBuilder()
              .SetKey("ns", "uri" + std::to_string(i))
              .SetSchema(std::string(kSchemaName))
              .Build())));
    }
  }

  // Seeds `count` IVF-buffered embeddings (one per document) into the delta
  // store and records them in seed_vectors_. Does NOT run maintenance, so the
  // hits remain in the delta-store cluster until a caller maintains the index.
  void SeedIvfEmbeddings(int count) {
    PutDocuments(count);
    std::mt19937 rng(/*seed=*/7);
    seed_vectors_.clear();
    seed_vectors_.reserve(count);
    for (int i = 0; i < count; ++i) {
      PropertyProto::VectorProto vector =
          GetRandomVector(rng, kModelSignature, kDimension);
      seed_vectors_.push_back(vector);
      ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
          BasicHit(/*section_id=*/0, /*document_id=*/i), vector,
          QUANTIZATION_TYPE_NONE, kSchemaName));
    }
    ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
    embedding_index_->set_last_added_document_id(count - 1);
    query_vector_ = seed_vectors_.empty()
                        ? CreateVector(kModelSignature, {1.0f, 0.0f, 0.0f})
                        : seed_vectors_[0];
  }

  static MaintainAnnIndexOptions MakeMaintainOptions() {
    MaintainAnnIndexOptions options;
    options.set_min_size_for_ivf(2);
    options.set_rebuild_threshold(0.0);
    options.mutable_mini_batch_k_means_options()->set_target_cluster_size(4);
    options.mutable_mini_batch_k_means_options()->set_min_num_iterations(1);
    return options;
  }

  libtextclassifier3::StatusOr<int> RunMaintainAllIvf() {
    return embedding_index_->MaintainAllIvf(*document_store_, *schema_store_,
                                            MakeMaintainOptions());
  }

  // After an IVF build, returns the total number of hits reachable across all
  // clusters closest to query_vector_. Returns -1 (and records a failure) if a
  // read errors.
  int CountAllIvfHits() {
    // k large enough to cover every cluster.
    auto cluster_ids_or = embedding_index_->GetClosestClusterIdsByDistance(
        query_vector_, /*k=*/static_cast<int>(seed_vectors_.size()));
    if (!cluster_ids_or.ok()) {
      ADD_FAILURE() << cluster_ids_or.status().error_message();
      return -1;
    }
    auto hits_or =
        GetEmbeddingHitsFromIndex(embedding_index_.get(), kDimension,
                                  kModelSignature, cluster_ids_or.ValueOrDie());
    if (!hits_or.ok()) {
      ADD_FAILURE() << hits_or.status().error_message();
      return -1;
    }
    return static_cast<int>(hits_or.ValueOrDie().size());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  std::string test_dir_;
  std::string embedding_index_dir_;
  std::string schema_store_dir_;
  std::string document_store_dir_;
  Clock clock_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<EmbeddingIndex> embedding_index_;
  uint32_t default_shard_id_ = 0;
  std::vector<PropertyProto::VectorProto> seed_vectors_;
  PropertyProto::VectorProto query_vector_;
};

// Many concurrent readers on linear-search storage.
TEST_F(EmbeddingIndexThreadSafetyTest, SimultaneousReads) {
  // Populate the index with 3 known, distinct embeddings (linear search).
  PropertyProto::VectorProto vector0 =
      CreateVector(kModelSignature, {0.1f, 0.2f, 0.3f});
  PropertyProto::VectorProto vector1 =
      CreateVector(kModelSignature, {0.4f, 0.5f, 0.6f});
  PropertyProto::VectorProto vector2 =
      CreateVector(kModelSignature, {0.7f, 0.8f, 0.9f});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector0,
      QUANTIZATION_TYPE_NONE, kSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector1,
      QUANTIZATION_TYPE_NONE, kSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), vector2,
      QUANTIZATION_TYPE_NONE, kSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  const std::vector<float> kExpectedRawData = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f,
                                               0.6f, 0.7f, 0.8f, 0.9f};

  // NOTE: std::vector<char> (not std::vector<bool>) because vector<bool>
  // bit-packs its elements, so concurrent writes to distinct indices would
  // share a byte and race. Distinct char elements occupy distinct bytes.
  std::vector<char> ok(kNumThreads, 0);
  auto callable = [&](int thread_id) {
    // Shared-lock read via GetAccessor (GetEmbeddingHitsFromIndex).
    auto hits_or = GetEmbeddingHitsFromIndex(embedding_index_.get(), kDimension,
                                             kModelSignature);
    if (!hits_or.ok() || hits_or.ValueOrDie().size() != 3) {
      return;
    }
    // Shared-lock read of the raw vector storage.
    std::vector<float> raw =
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_);
    if (raw != kExpectedRawData) {
      return;
    }
    // Shared-lock read via GetEmbeddingVector using a known hit + location.
    // Safe to dereference here because there are no concurrent writers.
    EmbeddingHit hit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                     /*location=*/6);
    auto vec_or = GetEmbeddingVector(embedding_index_.get(), hit, kDimension,
                                     default_shard_id_);
    if (!vec_or.ok()) {
      return;
    }
    const float* vec = vec_or.ValueOrDie();
    if (vec[0] != 0.7f || vec[1] != 0.8f || vec[2] != 0.9f) {
      return;
    }
    ok[thread_id] = 1;
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(callable, i);
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
    EXPECT_TRUE(ok[i]) << "Reader thread " << i << " observed unexpected data.";
  }
}

// Many concurrent readers on the post-IVF structure.
TEST_F(EmbeddingIndexThreadSafetyTest, SimultaneousReadsFromIvfClusters) {
  constexpr int kNumDocs = 20;
  SeedIvfEmbeddings(kNumDocs);
  ICING_ASSERT_OK(RunMaintainAllIvf());

  std::vector<char> ok(kNumThreads, 0);
  auto callable = [&](int thread_id) {
    auto cluster_ids_or = embedding_index_->GetClosestClusterIdsByDistance(
        query_vector_, /*k=*/kNumDocs);
    if (!cluster_ids_or.ok()) {
      return;
    }
    auto hits_or =
        GetEmbeddingHitsFromIndex(embedding_index_.get(), kDimension,
                                  kModelSignature, cluster_ids_or.ValueOrDie());
    if (hits_or.ok() &&
        hits_or.ValueOrDie().size() == static_cast<size_t>(kNumDocs)) {
      ok[thread_id] = 1;
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(callable, i);
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
    EXPECT_TRUE(ok[i]) << "Reader thread " << i
                       << " did not observe all IVF hits.";
  }
}

// Concurrent exclusive raw appends interleaved with shared snapshot reads.
TEST_F(EmbeddingIndexThreadSafetyTest, SimultaneousAppendAndRead) {
  // Initialize the storage shard with a single committed embedding so the
  // FileBackedVector exists.
  PropertyProto::VectorProto initial_vector =
      CreateVector(kModelSignature, {1.0f, 2.0f, 3.0f});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), initial_vector,
      QUANTIZATION_TYPE_NONE, kSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  constexpr int kNumAppenders = kNumThreads / 2;
  const std::vector<float> kAppendVector = {4.0f, 5.0f, 6.0f};

  auto callable = [&](int thread_id) {
    if (thread_id % 2 == 0) {
      // Exclusive-lock write.
      EmbeddingReference ref;
      ref.float_vector = kAppendVector.data();
      ICING_ASSERT_OK(AppendEmbeddingVector(embedding_index_.get(), ref,
                                            kDimension, default_shard_id_));
    } else {
      // Shared-lock read. The snapshot size must always be a whole number of
      // dimension-sized vectors -- never a torn/partial append.
      std::vector<float> raw = GetRawEmbeddingDataFromIndex(
          embedding_index_.get(), default_shard_id_);
      EXPECT_EQ(raw.size() % kDimension, 0u);
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(callable, i);
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
  }

  // After all appends, the storage must contain the initial vector plus one
  // appended vector per appender thread, with all data intact and in order.
  std::vector<float> raw =
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_);
  ASSERT_EQ(raw.size(), static_cast<size_t>((1 + kNumAppenders) * kDimension));
  EXPECT_EQ(raw[0], 1.0f);
  EXPECT_EQ(raw[1], 2.0f);
  EXPECT_EQ(raw[2], 3.0f);
  for (int i = 1; i <= kNumAppenders; ++i) {
    EXPECT_EQ(raw[i * kDimension], 4.0f);
    EXPECT_EQ(raw[i * kDimension + 1], 5.0f);
    EXPECT_EQ(raw[i * kDimension + 2], 6.0f);
  }
}

// Concurrent IVF maintenance interleaved with shared reads. This is the core
// scenario enabled by the flag: MaintainIvf's write-back takes an exclusive
// lock while queries take a shared lock. Reads must never crash or observe a
// torn state, and the index must remain consistent afterward.
TEST_F(EmbeddingIndexThreadSafetyTest, SimultaneousReadsDuringMaintenance) {
  constexpr int kNumDocs = 20;
  SeedIvfEmbeddings(kNumDocs);

  constexpr int kNumReaderThreads = 40;
  constexpr int kNumMaintainerThreads = 4;
  constexpr int kIterations = 20;

  // Reader threads repeatedly find the closest clusters and fetch their hits.
  // MaintainIvf may rebuild clusters concurrently, so a previously-returned
  // cluster id may become NOT_FOUND -- that is a valid, race-free outcome. The
  // only failures we care about are crashes, deadlocks, and internal errors.
  auto reader_callable = [&]() {
    for (int i = 0; i < kIterations; ++i) {
      auto cluster_ids_or = embedding_index_->GetClosestClusterIdsByDistance(
          query_vector_, /*k=*/2);
      ASSERT_TRUE(cluster_ids_or.ok())
          << cluster_ids_or.status().error_message();
      std::vector<uint32_t> cluster_ids =
          std::move(cluster_ids_or).ValueOrDie();
      if (cluster_ids.empty()) {
        // IVF hasn't been built yet; fall back to the linear-search cluster.
        cluster_ids = {embedding_util::kLinearSearchClusterId};
      }

      auto accessor_or =
          embedding_index_->GetAccessorForVector(query_vector_, cluster_ids);
      if (!accessor_or.ok()) {
        // NOT_FOUND is expected if the clusters were rebuilt out from under us.
        ASSERT_TRUE(absl_ports::IsNotFound(accessor_or.status()))
            << accessor_or.status().error_message();
        continue;
      }
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor> accessor =
          std::move(accessor_or).ValueOrDie();
      ICING_ASSERT_OK(accessor->AssertSharedLockHeld());
      // Drain the accessor to exercise reads under the held shared lock.
      while (true) {
        auto batch_or = accessor->GetNextHitsBatch();
        ASSERT_TRUE(batch_or.ok()) << batch_or.status().error_message();
        if (batch_or.ValueOrDie().empty()) {
          break;
        }
      }
    }
  };

  auto maintainer_callable = [&]() {
    for (int i = 0; i < kIterations; ++i) {
      // Concurrent MaintainAllIvf calls serialize via the internal guard;
      // overlapping calls simply return early. None should fail.
      ICING_ASSERT_OK(RunMaintainAllIvf());
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumReaderThreads + kNumMaintainerThreads);
  for (int i = 0; i < kNumReaderThreads; ++i) {
    threads.emplace_back(reader_callable);
  }
  for (int i = 0; i < kNumMaintainerThreads; ++i) {
    threads.emplace_back(maintainer_callable);
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // After all the concurrent activity, one final maintenance pass should leave
  // the index in a consistent, queryable state that returns all documents.
  ICING_ASSERT_OK(RunMaintainAllIvf());
  EXPECT_EQ(CountAllIvfHits(), kNumDocs);
}

// Multiple concurrent MaintainAllIvf calls.
TEST_F(EmbeddingIndexThreadSafetyTest, ConcurrentMaintainAllIvfCalls) {
  constexpr int kNumDocs = 20;
  SeedIvfEmbeddings(kNumDocs);

  constexpr int kMaintainThreads = 10;
  std::atomic<int> total_iterations_done{0};
  std::vector<char> ok(kMaintainThreads, 0);
  std::vector<std::thread> threads;
  threads.reserve(kMaintainThreads);
  for (int i = 0; i < kMaintainThreads; ++i) {
    threads.emplace_back([&, i]() {
      auto result = RunMaintainAllIvf();
      if (result.ok()) {
        ok[i] = 1;
        total_iterations_done.fetch_add(result.ValueOrDie());
      }
    });
  }
  for (int i = 0; i < kMaintainThreads; ++i) {
    threads[i].join();
    EXPECT_TRUE(ok[i]) << "MaintainAllIvf failed on thread " << i;
  }

  // At least one call must have performed real work (built the IVF).
  EXPECT_THAT(total_iterations_done.load(), Ge(1));

  // The index must be fully queryable and contain every document.
  ICING_ASSERT_OK(RunMaintainAllIvf());
  EXPECT_EQ(CountAllIvfHits(), kNumDocs);
}

// Concurrent GetClosestClusterIdsByDistance after an IVF build.
TEST_F(EmbeddingIndexThreadSafetyTest, ConcurrentGetClosestClusterIds) {
  constexpr int kNumDocs = 20;
  SeedIvfEmbeddings(kNumDocs);
  ICING_ASSERT_OK(RunMaintainAllIvf());

  // Reference result computed single-threaded before spawning threads.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_cluster_ids,
      embedding_index_->GetClosestClusterIdsByDistance(query_vector_, /*k=*/3));
  ASSERT_FALSE(expected_cluster_ids.empty());

  std::vector<char> ok(kNumThreads, 0);
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      auto result =
          embedding_index_->GetClosestClusterIdsByDistance(query_vector_,
                                                           /*k=*/3);
      if (result.ok() && result.ValueOrDie() == expected_cluster_ids) {
        ok[i] = 1;
      }
    });
  }
  for (int i = 0; i < kNumThreads; ++i) {
    threads[i].join();
    EXPECT_TRUE(ok[i]) << "Thread " << i
                       << " observed inconsistent cluster ids.";
  }
}

// Concurrent last_added_document_id() reads and set_last_added_document_id()
// writes.
TEST_F(EmbeddingIndexThreadSafetyTest, ConcurrentLastAddedDocumentIdReadWrite) {
  constexpr int kNumSeed = 10;
  SeedIvfEmbeddings(kNumSeed);
  // Baseline last_added_document_id is kNumSeed - 1.

  int expected_max = kNumSeed - 1;
  for (int i = 0; i < kNumThreads; ++i) {
    if (i % 2 != 0) {
      expected_max = std::max(expected_max, kNumSeed + i);
    }
  }

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      if (i % 2 == 0) {
        // Reader: must always see a valid, monotonically-safe value.
        DocumentId value = embedding_index_->last_added_document_id();
        EXPECT_THAT(value, Ge(kNumSeed - 1));
      } else {
        // Writer: bump the id.
        embedding_index_->set_last_added_document_id(kNumSeed + i);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(embedding_index_->last_added_document_id(), expected_max);
}

}  // namespace lib
}  // namespace icing
