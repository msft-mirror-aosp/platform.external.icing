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

#include <ctime>

namespace icing {
namespace lib {

// Wrapper around real-time clock functions. This is separated primarily so
// tests can override this clock and inject it into the class under test.
//
// A few things to note about std::time_t :
// From cppreference:
//   "Although not defined, this is almost always an integral value holding the
//   number of seconds (not counting leap seconds) since 00:00, Jan 1 1970 UTC,
//   corresponding to POSIX time"
//
// From Wikipedia:
//   "ISO C defines time_t as an arithmetic type, but does not specify any
//   particular type, range, resolution, or encoding for it. Also unspecified
//   are the meanings of arithmetic operations applied to time values."
class Clock {
 public:
  virtual ~Clock() {}

  // Returns:
  //   The current time defined by the clock on success
  //   std::time_t(-1) on error
  virtual std::time_t GetCurrentSeconds() const { return std::time(nullptr); }
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_CLOCK_H_
