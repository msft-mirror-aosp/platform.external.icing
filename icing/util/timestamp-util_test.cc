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

#include "third_party/icing/util/timestamp-util.h"

#include <cstdint>
#include <limits>

#include "testing/base/public/gmock.h"
#include "testing/base/public/gunit.h"

namespace icing {
namespace lib {
namespace timestamp_util {

namespace {

using ::testing::Eq;

TEST(TimestampUtilTest, CalculateRawExpirationTimestampMs) {
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/0,
                                                /*ttl_ms=*/1000),
              Eq(1000));
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/1000,
                                                /*ttl_ms=*/1000),
              Eq(2000));
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/2000,
                                                /*ttl_ms=*/1000),
              Eq(3000));
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/5000,
                                                /*ttl_ms=*/1000),
              Eq(6000));
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/5000,
                                                /*ttl_ms=*/3000),
              Eq(8000));
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/5000,
                                                /*ttl_ms=*/10000),
              Eq(15000));
}

TEST(TimestampUtilTest,
     CalculateRawExpirationTimestampMs_zeroTtlShouldReturnInt64Max) {
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/1000,
                                                /*ttl_ms=*/0),
              Eq(std::numeric_limits<int64_t>::max()));
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/2000,
                                                /*ttl_ms=*/0),
              Eq(std::numeric_limits<int64_t>::max()));
  EXPECT_THAT(CalculateRawExpirationTimestampMs(/*creation_timestamp_ms=*/5000,
                                                /*ttl_ms=*/0),
              Eq(std::numeric_limits<int64_t>::max()));
}

TEST(TimestampUtilTest,
     CalculateRawExpirationTimestampMs_shouldPreventOverflow) {
  EXPECT_THAT(
      CalculateRawExpirationTimestampMs(
          /*creation_timestamp_ms=*/std::numeric_limits<int64_t>::max() - 2,
          /*ttl_ms=*/2),
      Eq(std::numeric_limits<int64_t>::max()));
  EXPECT_THAT(
      CalculateRawExpirationTimestampMs(
          /*creation_timestamp_ms=*/std::numeric_limits<int64_t>::max() - 2,
          /*ttl_ms=*/100),
      Eq(std::numeric_limits<int64_t>::max()));
  EXPECT_THAT(
      CalculateRawExpirationTimestampMs(
          /*creation_timestamp_ms=*/std::numeric_limits<int64_t>::max() - 1,
          /*ttl_ms=*/100000),
      Eq(std::numeric_limits<int64_t>::max()));
}

}  // namespace

}  // namespace timestamp_util
}  // namespace lib
}  // namespace icing
