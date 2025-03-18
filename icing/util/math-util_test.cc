// Copyright (C) 2024 Google LLC
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

#include "icing/util/math-util.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;

TEST(MathUtilTest, NextPowerOf2) {
  EXPECT_THAT(math_util::NextPowerOf2(0), Eq(1));
  EXPECT_THAT(math_util::NextPowerOf2(1), Eq(1));
  EXPECT_THAT(math_util::NextPowerOf2(2), Eq(2));
  EXPECT_THAT(math_util::NextPowerOf2(3), Eq(4));
  EXPECT_THAT(math_util::NextPowerOf2(4), Eq(4));
  EXPECT_THAT(math_util::NextPowerOf2(5), Eq(8));
  EXPECT_THAT(math_util::NextPowerOf2(6), Eq(8));
  EXPECT_THAT(math_util::NextPowerOf2(7), Eq(8));
  EXPECT_THAT(math_util::NextPowerOf2(8), Eq(8));
  EXPECT_THAT(math_util::NextPowerOf2(9), Eq(16));
  EXPECT_THAT(math_util::NextPowerOf2(16), Eq(16));
  EXPECT_THAT(math_util::NextPowerOf2(17), Eq(32));

  // 2^31 - 1
  EXPECT_THAT(math_util::NextPowerOf2((UINT32_C(1) << 31) - 1),
              Eq(UINT32_C(1) << 31));

  // 2^31
  EXPECT_THAT(math_util::NextPowerOf2(UINT32_C(1) << 31),
              Eq(UINT32_C(1) << 31));
}

}  // namespace

}  // namespace lib
}  // namespace icing
