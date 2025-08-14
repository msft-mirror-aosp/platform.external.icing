// Copyright (C) 2025 Google LLC
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

#include <signal.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <random>

#include "testing/base/public/gunit.h"
#include "third_party/icing/monkey_test/icing-monkey-test-runner.h"
#include "third_party/icing/monkey_test/monkey-test-util.h"
#include "third_party/icing/portable/platform.h"
#include "third_party/icing/proto/debug.proto.h"
#include "third_party/icing/schema/section.h"
#include "third_party/icing/util/logging.h"

namespace icing {
namespace lib {

constexpr uint32_t kNumCrashesToSimulate = 5;
constexpr uint32_t kMinRunningTimePerCrashSeconds = 5;
constexpr uint32_t kMaxRunningTimePerCrashSeconds = 15;

void RunMonkeyTest(uint32_t seed, bool is_first_run, uint32_t num_iterations) {
  IcingMonkeyTestRunnerConfiguration config(
      seed,
      /*num_types=*/30,
      /*num_namespaces=*/100,
      /*num_uris=*/1000,
      /*index_merge_size=*/1024 * 1024,
      /*initialize_by_existing_data=*/!is_first_run);
  config.possible_num_properties = {0,
                                    1,
                                    2,
                                    4,
                                    8,
                                    16,
                                    kTotalNumSections / 2,
                                    kTotalNumSections,
                                    kTotalNumSections + 1,
                                    kTotalNumSections * 2};
  config.possible_num_tokens = {0, 1, 4, 16, 64, 256};
  config.possible_num_vectors = {0, 1, 4};
  config.possible_vector_dimensions = {128, 512, 768};
  config.monkey_api_schedules = {
      {&IcingMonkeyTestRunner::DoPut, 500},
      {&IcingMonkeyTestRunner::DoSearch, 200},
      {&IcingMonkeyTestRunner::DoGet, 70},
      {&IcingMonkeyTestRunner::DoGetAllNamespaces, 50},
      {&IcingMonkeyTestRunner::DoDelete, 50},
      {&IcingMonkeyTestRunner::DoDeleteByNamespace, 50},
      {&IcingMonkeyTestRunner::DoDeleteBySchemaType, 45},
      {&IcingMonkeyTestRunner::DoDeleteByQuery, 20},
      {&IcingMonkeyTestRunner::DoOptimize, 5},
      {&IcingMonkeyTestRunner::DoUpdateSchema, 5},
      {&IcingMonkeyTestRunner::ReloadFromDisk, 5}};

  std::unique_ptr<IcingMonkeyTestRunner> runner =
      std::make_unique<IcingMonkeyTestRunner>(config);
  ASSERT_NO_FATAL_FAILURE(runner->Initialize());
  ASSERT_NO_FATAL_FAILURE(runner->Run(num_iterations));
  runner.reset();
}

TEST(IcingSearchEngineMonkeyCrashSimulationTest, MonkeyTest) {
  uint32_t seed = std::random_device()();
  MonkeyTestRandomEngine random(seed);
  ICING_LOG(INFO) << "Monkey test crash simulation started with seed: " << seed;

  bool is_first_run = true;
  for (int i = 0; i < kNumCrashesToSimulate; ++i) {
    uint32_t child_seed = random();
    pid_t pid = fork();
    ASSERT_TRUE(pid >= 0) << "fork() failed!";

    // Child process
    if (pid == 0) {
      // Run indefinitely until the parent process kills the child process.
      ICING_LOG(INFO) << "Child process " << i << " started running";
      ASSERT_NO_FATAL_FAILURE(
          RunMonkeyTest(child_seed, is_first_run, /*num_iterations=*/
                        std::numeric_limits<uint32_t>::max()));
      _exit(0);
    }

    // Randomly generate the running time for each crash.
    std::uniform_int_distribution<uint32_t>
        running_time_per_crash_seconds_distribution(
            kMinRunningTimePerCrashSeconds, kMaxRunningTimePerCrashSeconds);
    sleep(running_time_per_crash_seconds_distribution(random));

    // Check if the child process has exited.
    int status = 0;
    pid_t ret = waitpid(pid, &status, WNOHANG);
    if (ret == pid) {
      // The child process has already exited for some reason, which is not
      // expected. We should check the reason and fail the overall test.
      if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        FAIL() << "Monkey test in the child process exited with code " << code;
      } else if (WIFSIGNALED(status)) {
        FAIL()
            << "Monkey test in the child process was signaled early with sig="
            << WTERMSIG(status);
      }

      FAIL() << "Monkey test in the child process ended in unknown reason";
    } else if (ret != 0) {
      FAIL() << "waitpid() error!";
    }

    // The child process is still running, which is expected. We can now kill
    // the child process to simulate a crash.
    if (kill(pid, SIGKILL) != 0) {
      FAIL() << "Failed to kill child process " << i;
    }
    waitpid(pid, &status, 0);
    ICING_LOG(INFO) << "Child process killed.";

    is_first_run = false;
  }

  // Run the monkey test after the crash. Icing search engine should be able to
  // recover from the crash and continue running normally.
  ICING_LOG(INFO) << "Running final monkey test after crash simulation.";
  uint32_t num_iterations = IsAndroidArm() ? 200 : 1000;
  ASSERT_NO_FATAL_FAILURE(
      RunMonkeyTest(/*seed=*/random(), is_first_run, num_iterations));
}

}  // namespace lib
}  // namespace icing
