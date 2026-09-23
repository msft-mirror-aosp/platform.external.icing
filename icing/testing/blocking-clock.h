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

#ifndef ICING_TESTING_BLOCKING_CLOCK_H_
#define ICING_TESTING_BLOCKING_CLOCK_H_

#include <condition_variable>  // NOLINT
#include <cstdint>
#include <memory>

#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/thread_annotations.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {

// A Clock implementation for tests that need deterministic control over when a
// component under test reads the time. Any Timer handed out by this clock will,
// on the next elapsed-time query after blocking is enabled, block the calling
// thread until it is explicitly unblocked. This makes it possible to pause a
// background operation at a well-defined point (whenever it queries a timer)
// and deterministically interleave it with work happening on other threads.
//
// Typical usage:
//   1. Call EnableBlockOnTimerQuery() right before triggering the operation.
//   2. Call WaitUntilBlocked() to block until that operation reaches a timer.
//   3. Perform whatever concurrent work the test needs.
//   4. Call Unblock() to let the paused thread resume.
//
// Blocking is disabled by default so that initialization and setup (which also
// query timers) are not accidentally blocked, and must be explicitly enabled
// via EnableBlockOnTimerQuery().
class BlockingClock : public Clock {
 public:
  BlockingClock() = default;

  class BlockingTimer : public Timer {
   public:
    explicit BlockingTimer(const BlockingClock* clock) : clock_(clock) {}

    int64_t GetElapsedMilliseconds() const override {
      clock_->BlockIfNeeded();
      return 0;
    }

    int64_t GetElapsedNanoseconds() const override {
      clock_->BlockIfNeeded();
      return 0;
    }

   private:
    const BlockingClock* clock_;
  };

  std::unique_ptr<Timer> GetNewTimer() const override {
    return std::make_unique<BlockingTimer>(this);
  }

  int64_t GetSystemTimeMilliseconds() const override { return 0; }

  // Tells the clock to block the thread the next time a timer is queried.
  // Note: This will only block the *next* caller. Once that caller is blocked
  // and subsequently unblocked, all future callers will not be blocked unless
  // this is called again.
  void EnableBlockOnTimerQuery() {
    absl_ports::unique_lock l(&mutex_);
    should_block_ = true;
    is_blocked_ = false;
  }

  // Blocks the calling thread until a timer created by this clock is queried.
  void WaitUntilBlocked() {
    absl_ports::unique_lock l(&mutex_);
    while (!is_blocked_) {
      cv_.wait(l);
    }
  }

  // Unblocks the timer, allowing the blocked thread to continue execution.
  void Unblock() {
    absl_ports::unique_lock l(&mutex_);
    should_block_ = false;
    cv_.notify_all();
  }

 private:
  void BlockIfNeeded() const {
    absl_ports::unique_lock l(&mutex_);
    if (!should_block_ || is_blocked_) {
      return;
    }
    is_blocked_ = true;
    cv_.notify_all();
    while (should_block_) {
      cv_.wait(l);
    }
  }

  mutable absl_ports::shared_mutex mutex_;
  mutable std::condition_variable_any cv_;
  mutable bool should_block_ ICING_GUARDED_BY(mutex_) = false;
  mutable bool is_blocked_ ICING_GUARDED_BY(mutex_) = false;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_BLOCKING_CLOCK_H_
