// Copyright (C) 2023 Google LLC
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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "testing/base/public/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/file/destructible-directory.h"
#include "icing/file/filesystem.h"
#include "icing/index/numeric/integer-index-storage.h"
#include "icing/index/numeric/posting-list-integer-index-serializer.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/numeric/number-generator.h"
#include "icing/testing/numeric/uniform-distribution-integer-generator.h"
#include "icing/testing/tmp-directory.h"

// Run on a Linux workstation:
//   $ blaze build -c opt --dynamic_mode=off --copt=-gmlt
//   //icing/index/numeric:integer-index-storage_benchmark
//
//   $ blaze-bin/icing/index/numeric/integer-index-storage_benchmark
//   --benchmark_filter=all --benchmark_memory_usage
//
// Run on an Android device:
//   $ blaze build --copt="-DGOOGLE_COMMANDLINEFLAGS_FULL_API=1"
//   --config=android_arm64 -c opt --dynamic_mode=off --copt=-gmlt
//   //icing/index/numeric:integer-index-storage_benchmark
//
//   $ adb push
//   blaze-bin/icing/index/numeric/integer-index-storage_benchmark
//   /data/local/tmp/
//
//   $ adb shell /data/local/tmp/integer-index-storage_benchmark
//   --benchmark_filter=all

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;
using ::testing::SizeIs;

static constexpr SectionId kDefaultSectionId = 12;
static constexpr int kDefaultSeed = 12345;

enum DistributionTypeEnum {
  kUniformDistribution,
};

class IntegerIndexStorageBenchmark {
 public:
  Filesystem filesystem;
  std::string working_path;

  PostingListIntegerIndexSerializer posting_list_serializer;

  explicit IntegerIndexStorageBenchmark()
      : working_path(GetTestTempDir() + "/integer_index_benchmark") {}

  ~IntegerIndexStorageBenchmark() {
    filesystem.DeleteDirectoryRecursively(working_path.c_str());
  }
};

libtextclassifier3::StatusOr<std::unique_ptr<NumberGenerator<int64_t>>>
CreateIntegerGenerator(DistributionTypeEnum distribution_type, int seed,
                       int num_keys) {
  switch (distribution_type) {
    case DistributionTypeEnum::kUniformDistribution:
      // Since the collision # follows poisson distribution with lambda =
      // (num_keys / range), we set the range 10x (lambda = 0.1) to avoid too
      // many collisions.
      //
      // Distribution:
      // - keys in range being picked for 0 times: 90.5%
      // - keys in range being picked for 1 time: 9%
      // - keys in range being picked for 2 times: 0.45%
      // - keys in range being picked for 3 times: 0.015%
      //
      // For example, num_keys = 1M, range = 10M. Then there will be ~904837
      // unique keys, 45242 keys being picked twice, 1508 keys being picked
      // thrice ...
      return std::make_unique<UniformDistributionIntegerGenerator<int64_t>>(
          seed, /*range_lower=*/0,
          /*range_upper=*/static_cast<int64_t>(num_keys) * 10 - 1);
    default:
      return absl_ports::InvalidArgumentError("Unknown type");
  }
}

void BM_Index(benchmark::State& state) {
  DistributionTypeEnum distribution_type =
      static_cast<DistributionTypeEnum>(state.range(0));
  int num_keys = state.range(1);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumberGenerator<int64_t>> generator,
      CreateIntegerGenerator(distribution_type, kDefaultSeed, num_keys));
  std::vector<int64_t> keys(num_keys);
  for (int i = 0; i < num_keys; ++i) {
    keys[i] = generator->Generate();
  }

  IntegerIndexStorageBenchmark benchmark;
  for (auto _ : state) {
    state.PauseTiming();
    benchmark.filesystem.DeleteDirectoryRecursively(
        benchmark.working_path.c_str());
    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<IntegerIndexStorage> storage,
                               IntegerIndexStorage::Create(
                                   benchmark.filesystem, benchmark.working_path,
                                   IntegerIndexStorage::Options(),
                                   &benchmark.posting_list_serializer));
    state.ResumeTiming();

    for (int i = 0; i < num_keys; ++i) {
      ICING_ASSERT_OK(storage->AddKeys(static_cast<DocumentId>(i),
                                       kDefaultSectionId, {keys[i]}));
    }
    ICING_ASSERT_OK(storage->PersistToDisk());

    state.PauseTiming();
    storage.reset();
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Index)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 10)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 11)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 12)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 13)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 14)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 15)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 16)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 17)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 18)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 19)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 20);

void BM_BatchIndex(benchmark::State& state) {
  DistributionTypeEnum distribution_type =
      static_cast<DistributionTypeEnum>(state.range(0));
  int num_keys = state.range(1);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumberGenerator<int64_t>> generator,
      CreateIntegerGenerator(distribution_type, kDefaultSeed, num_keys));
  std::vector<int64_t> keys(num_keys);
  for (int i = 0; i < num_keys; ++i) {
    keys[i] = generator->Generate();
  }

  IntegerIndexStorageBenchmark benchmark;
  for (auto _ : state) {
    state.PauseTiming();
    benchmark.filesystem.DeleteDirectoryRecursively(
        benchmark.working_path.c_str());
    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<IntegerIndexStorage> storage,
                               IntegerIndexStorage::Create(
                                   benchmark.filesystem, benchmark.working_path,
                                   IntegerIndexStorage::Options(),
                                   &benchmark.posting_list_serializer));
    std::vector<int64_t> keys_copy(keys);
    state.ResumeTiming();

    ICING_ASSERT_OK(storage->AddKeys(static_cast<DocumentId>(0),
                                     kDefaultSectionId, std::move(keys_copy)));
    ICING_ASSERT_OK(storage->PersistToDisk());

    state.PauseTiming();
    storage.reset();
    state.ResumeTiming();
  }
}
BENCHMARK(BM_BatchIndex)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 10)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 11)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 12)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 13)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 14)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 15)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 16)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 17)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 18)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 19)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 20);

void BM_ExactQuery(benchmark::State& state) {
  DistributionTypeEnum distribution_type =
      static_cast<DistributionTypeEnum>(state.range(0));
  int num_keys = state.range(1);

  IntegerIndexStorageBenchmark benchmark;
  benchmark.filesystem.DeleteDirectoryRecursively(
      benchmark.working_path.c_str());
  DestructibleDirectory ddir(&benchmark.filesystem, benchmark.working_path);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(benchmark.filesystem, benchmark.working_path,
                                  IntegerIndexStorage::Options(),
                                  &benchmark.posting_list_serializer));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumberGenerator<int64_t>> generator,
      CreateIntegerGenerator(distribution_type, kDefaultSeed, num_keys));
  std::unordered_map<int64_t, std::vector<DocumentId>> keys;
  for (int i = 0; i < num_keys; ++i) {
    int64_t key = generator->Generate();
    keys[key].push_back(static_cast<DocumentId>(i));
    ICING_ASSERT_OK(
        storage->AddKeys(static_cast<DocumentId>(i), kDefaultSectionId, {key}));
  }
  ICING_ASSERT_OK(storage->PersistToDisk());

  for (auto _ : state) {
    int64_t exact_query_key = generator->Generate();
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocHitInfoIterator> iterator,
        storage->GetIterator(/*query_key_lower=*/exact_query_key,
                             /*query_key_upper=*/exact_query_key));
    int cnt = 0;
    while (iterator->Advance().ok()) {
      benchmark::DoNotOptimize(iterator->doc_hit_info());
      ++cnt;
    }

    const auto it = keys.find(exact_query_key);
    if (it == keys.end()) {
      ASSERT_THAT(cnt, Eq(0));
    } else {
      ASSERT_THAT(it->second, SizeIs(cnt));
    }
  }
}
BENCHMARK(BM_ExactQuery)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 10)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 11)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 12)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 13)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 14)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 15)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 16)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 17)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 18)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 19)
    ->ArgPair(DistributionTypeEnum::kUniformDistribution, 1 << 20);

}  // namespace

}  // namespace lib
}  // namespace icing
