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
#include "icing/schema/section.h"

namespace icing {
namespace lib {

void TestJoinDeletePropagation(uint32_t seed) {
  IcingMonkeyTestRunnerConfiguration config(
      /*seed=*/seed,
      /*num_types=*/5,
      /*num_namespaces=*/10,
      /*num_uris=*/10,
      /*index_merge_size=*/1024 * 512,
      /*initialize_by_existing_data=*/false);

  config.possible_num_properties = {
      0, 1, 2, 4, 8, 16, kTotalNumSections / 2, kTotalNumSections};
  config.possible_num_tokens = {0, 1, 4, 16};
  config.possible_num_vectors = {0, 1};
  config.possible_vector_dimensions = {8, 16};
  config.possible_ref_qualified_id_random_spaces = {
      {.namespace_l = 0,
       .namespace_r = 4,
       .uri_l = 0,
       .uri_r = 25}};  // 100 possible qualified ids.
  config.enable_join_delete_propagation = true;

  config.monkey_api_schedules = {{&IcingMonkeyTestRunner::DoPut, 600},
                                 {&IcingMonkeyTestRunner::DoJoinSearch, 50},
                                 {&IcingMonkeyTestRunner::DoSearch, 50},
                                 {&IcingMonkeyTestRunner::DoDelete, 300},
                                 {&IcingMonkeyTestRunner::DoOptimize, 10},
                                 {&IcingMonkeyTestRunner::DoUpdateSchema, 10},
                                 {&IcingMonkeyTestRunner::DoPersistToDisk, 10},
                                 {&IcingMonkeyTestRunner::ReloadFromDisk, 20}};
  uint32_t num_iterations = IsAndroidArm() ? 1000 : 5000;
  IcingMonkeyTestRunner runner(std::move(config));
  ASSERT_NO_FATAL_FAILURE(runner.Initialize());
  ASSERT_NO_FATAL_FAILURE(runner.Run(num_iterations));
}

FUZZ_TEST(IcingSearchEngineMonkeyTest, TestJoinDeletePropagation);

// To run the monkey test many times locally, do not rely on the fuzz test,
// since it would generate similar seeds multiple times. Instead, use this
// target to run it many times:
// blaze test -c opt --runs_per_test=1000 \
//   --test_filter=*LocalMonkeyTest* \
//   --test_arg=--gunit_also_run_disabled_tests \
//   //icing/monkey_test/test_suites:join-delete-propagation_test
TEST(DISABLED_IcingSearchEngineMonkeyTest, LocalMonkeyTest) {
  uint32_t seed = std::random_device()();  // NOLINT
  ASSERT_NO_FATAL_FAILURE(TestJoinDeletePropagation(seed));
}

}  // namespace lib
}  // namespace icing
