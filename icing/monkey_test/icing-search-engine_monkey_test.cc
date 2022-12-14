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

#include "gtest/gtest.h"
#include "icing/monkey_test/icing-monkey-test-runner.h"
#include "icing/portable/platform.h"

namespace icing {
namespace lib {

TEST(IcingSearchEngineMonkeyTest, MonkeyTest) {
  uint32_t num_iterations = IsAndroidArm() ? 1000 : 5000;
  IcingMonkeyTestRunner runner;
  ASSERT_NO_FATAL_FAILURE(runner.CreateIcingSearchEngineWithSchema());
  ASSERT_NO_FATAL_FAILURE(runner.Run(num_iterations));
}

}  // namespace lib
}  // namespace icing
