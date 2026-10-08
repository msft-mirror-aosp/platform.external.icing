// Copyright (C) 2022 Google LLC
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

void TestGeneralApis(uint32_t seed) {
  IcingMonkeyTestRunnerConfiguration config(
      /*seed=*/seed,
      /*num_types=*/30,
      /*num_namespaces=*/100,
      /*num_uris=*/1000,
      /*index_merge_size=*/1024 * 512,
      /*initialize_by_existing_data=*/false);
  config.possible_num_tokens = {0, 1, 4, 16, 64, 256};
  config.possible_num_vectors = {0, 1, 4, 8};
  config.possible_vector_dimensions = {8, 16, 128, 512, 768};
  config.monkey_api_schedules = {
      {&IcingMonkeyTestRunner::DoPut, 500},
      {&IcingMonkeyTestRunner::DoSearch, 200},
      {&IcingMonkeyTestRunner::DoGet, 50},
      {&IcingMonkeyTestRunner::DoGetAllNamespaces, 50},
      {&IcingMonkeyTestRunner::DoDelete, 50},
      {&IcingMonkeyTestRunner::DoDeleteByNamespace, 50},
      {&IcingMonkeyTestRunner::DoDeleteBySchemaType, 40},
      {&IcingMonkeyTestRunner::DoDeleteByQuery, 20},
      {&IcingMonkeyTestRunner::DoMaintainAnnIndex, 5},
      {&IcingMonkeyTestRunner::DoOptimize, 4},
      {&IcingMonkeyTestRunner::DoUpdateSchema, 4},
      {&IcingMonkeyTestRunner::DoPersistToDisk, 4},
      {&IcingMonkeyTestRunner::DoGetDebugInfo, 3},
      {&IcingMonkeyTestRunner::ReloadFromDisk, 20},
      {&IcingMonkeyTestRunner::DoGetNextPage, 100}};
  uint32_t num_iterations = IsAndroidArm() ? 1000 : 5000;
  IcingMonkeyTestRunner runner(std::move(config));
  ASSERT_NO_FATAL_FAILURE(runner.Initialize());
  ASSERT_NO_FATAL_FAILURE(runner.Run(num_iterations));
}

FUZZ_TEST(IcingSearchEngineMonkeyTest, TestGeneralApis);

// To run the monkey test many times locally, do not rely on the fuzz test,
// since it would generate similar seeds multiple times. Instead, use this
// target to run it many times:
// blaze test -c opt --runs_per_test=1000 \
//   --test_filter=*LocalMonkeyTest* \
//   --test_arg=--gunit_also_run_disabled_tests \
//   //icing/monkey_test/test_suites:general-apis_test
TEST(DISABLED_IcingSearchEngineMonkeyTest, LocalMonkeyTest) {
  uint32_t seed = std::random_device()();  // NOLINT
  ASSERT_NO_FATAL_FAILURE(TestGeneralApis(seed));
}

}  // namespace lib
}  // namespace icing
