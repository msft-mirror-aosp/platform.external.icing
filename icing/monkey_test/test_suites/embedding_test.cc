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

#include <cstdint>
#include <random>
#include <utility>

#include "gtest/gtest.h"
#include "testing/fuzzing/fuzztest.h"
#include "icing/monkey_test/icing-monkey-test-runner.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/portable/platform.h"
#include "icing/proto/debug.pb.h"

namespace icing {
namespace lib {

void TestEmbeddingApis(uint32_t seed) {
  IcingMonkeyTestRunnerConfiguration config(
      /*seed=*/seed,
      /*num_types=*/30,
      /*num_namespaces=*/100,
      /*num_uris=*/1000,
      /*index_merge_size=*/1024 * 512,
      /*initialize_by_existing_data=*/false);
  // Use less term tokens, since the test is embedding focused.
  config.possible_num_tokens = {0, 1};
  config.possible_num_vectors = {0, 1, 4, 8, 16, 32};
  config.possible_vector_dimensions = {8, 16, 128, 512, 768};
  config.possible_num_shards = {1, 16, 32, 64};
  config.monkey_api_schedules = {
      {&IcingMonkeyTestRunner::DoPut, 500},
      {&IcingMonkeyTestRunner::DoSearch, 300},
      {&IcingMonkeyTestRunner::DoDelete, 150},
      {&IcingMonkeyTestRunner::DoMaintainAnnIndex, 30},
      {&IcingMonkeyTestRunner::DoOptimize, 5},
      {&IcingMonkeyTestRunner::DoUpdateSchema, 5},
      {&IcingMonkeyTestRunner::DoPersistToDisk, 5},
      {&IcingMonkeyTestRunner::ReloadFromDisk, 5}};
  uint32_t num_iterations = IsAndroidArm() ? 1000 : 5000;
  IcingMonkeyTestRunner runner(std::move(config));
  ASSERT_NO_FATAL_FAILURE(runner.Initialize());
  ASSERT_NO_FATAL_FAILURE(runner.Run(num_iterations));
}

FUZZ_TEST(IcingSearchEngineMonkeyTest, TestEmbeddingApis);

// To run the monkey test many times locally, do not rely on the fuzz test,
// since it would generate similar seeds multiple times. Instead, use this
// target to run it many times:
// blaze test -c opt --runs_per_test=1000 \
//   --test_filter=*LocalMonkeyTest* \
//   --test_arg=--gunit_also_run_disabled_tests \
//   //icing/monkey_test/test_suites:embedding_test
TEST(DISABLED_IcingSearchEngineMonkeyTest, LocalMonkeyTest) {
  uint32_t seed = std::random_device()();  // NOLINT
  ASSERT_NO_FATAL_FAILURE(TestEmbeddingApis(seed));
}

}  // namespace lib
}  // namespace icing
