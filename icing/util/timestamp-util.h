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

#ifndef ICING_UTIL_TIMESTAMP_UTIL_H_
#define ICING_UTIL_TIMESTAMP_UTIL_H_

#include <cstdint>

namespace icing {
namespace lib {

namespace timestamp_util {

// Calculates the (raw) expiration timestamp of a document given its creation
// timestamp and time-to-live (TTL) in milliseconds.
//
// If the TTL is 0, the document should never expire and the function will
// return INT64_MAX as the expiration timestamp.
//
// If an overflow occurs, the function will return INT64_MAX.
//
// REQUIRES: creation_timestamp_ms >= 0 && ttl_ms >= 0.
int64_t CalculateRawExpirationTimestampMs(int64_t creation_timestamp_ms,
                                          int64_t ttl_ms);

}  // namespace timestamp_util

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_TIMESTAMP_UTIL_H_
