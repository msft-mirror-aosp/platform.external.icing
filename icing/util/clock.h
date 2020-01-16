// Copyright (C) 2019 Google LLC
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

#ifndef ICING_UTIL_CLOCK_H_
#define ICING_UTIL_CLOCK_H_

#include <chrono>  // NOLINT. Abseil library is not available in AOSP so we have
                   // to use chrono to get current time in milliseconds.

namespace icing {
namespace lib {

// Wrapper around real-time clock functions. This is separated primarily so
// tests can override this clock and inject it into the class under test.
class Clock {
 public:
  virtual ~Clock() = default;

  // Returns the current time in milliseconds, it's guaranteed that the return
  // value is non-negative.
  virtual int64_t GetSystemTimeMilliseconds() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_CLOCK_H_
