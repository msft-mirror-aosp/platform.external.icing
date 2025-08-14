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

namespace icing {
namespace lib {

namespace timestamp_util {

int64_t CalculateRawExpirationTimestampMs(int64_t creation_timestamp_ms,
                                          int64_t ttl_ms) {
  if (ttl_ms == 0) {
    // Special case where a TTL of 0 indicates the document should never
    // expire. int64_t max, interpreted as seconds since epoch, represents
    // some point in the year 292,277,026,596. So we're probably ok to use
    // this as "never reaching this point".
    return std::numeric_limits<int64_t>::max();
  }

  int64_t expiration_timestamp_ms;
  if (__builtin_add_overflow(creation_timestamp_ms, ttl_ms,
                             &expiration_timestamp_ms)) {
    // Overflow detected. Treat overflow as the same behavior of just int64_t
    // max
    return std::numeric_limits<int64_t>::max();
  }

  return expiration_timestamp_ms;
}

}  // namespace timestamp_util

}  // namespace lib
}  // namespace icing
