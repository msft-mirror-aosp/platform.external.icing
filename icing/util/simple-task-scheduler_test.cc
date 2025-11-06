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

#include "icing/util/simple-task-scheduler.h"

#include <atomic>
#include <chrono>  // NOLINT
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>  // NOLINT

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/fake-clock.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;

constexpr SimpleTaskScheduler::TaskId kTaskId = 0x12345678;

// The epsilon for sleep() in milliseconds. There may be a small delay between
// the scheduled task firing time and std::this_thread::sleep_for in the unit
// test, so we need to sleep a little longer to make sure the task completes.
constexpr int64_t kSleepMsEps = 100;

// Simulate a class holds and uses the scheduler object to fire its own API
// call.
class MainClass {
 public:
  explicit MainClass(const Clock& clock) : clock_(clock) {}

  void Initialize() {
    std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

    scheduler_ = SimpleTaskScheduler::Create(clock_);
    scheduler_->ScheduleAt(kTaskId, CreateTask(),
                           clock_.GetSystemTimeMilliseconds() + 1000);
  }

  void FooApi() {
    std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

    ++counter_;
    if (scheduler_ != nullptr) {
      // Re-schedule a new task to fire in 1 second.
      scheduler_->ScheduleAt(kTaskId, CreateTask(),
                             clock_.GetSystemTimeMilliseconds() + 1000);
    }
  }

  int GetCounter() {
    std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

    return counter_;
  }

 private:
  std::function<void()> CreateTask() {
    return [this]() { FooApi(); };
  }

  std::mutex mutex_;  // NOLINT

  const Clock& clock_;  // Does not own.
  std::unique_ptr<SimpleTaskScheduler> scheduler_;
  int counter_ = 0;
};

TEST(SimpleTaskSchedulerTest, SimpleScheduleAndExecute) {
  Clock clock;
  int counter = 0;

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule the task to fire in 1 second.
  scheduler->ScheduleAt(
      kTaskId, /*task=*/[&counter]() { ++counter; },
      clock.GetSystemTimeMilliseconds() + 1000);

  // After 1 second, the task should fire and increment the counter.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));
}

TEST(SimpleTaskSchedulerTest, ScheduleAt_rescheduleTask) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() { ++counter; };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire in 3 seconds.
  scheduler->ScheduleAt(kTaskId, task,
                        clock.GetSystemTimeMilliseconds() + 3000);

  // Sleep for 100 ms and reschedule the task (with the same id) to fire in 1
  // second.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  scheduler->ScheduleAt(kTaskId, task,
                        clock.GetSystemTimeMilliseconds() + 1000);

  // - After 1 second, the task should fire and increment the counter.
  // - Although ScheduleAt() interrupts wait_for(), the task should not be
  //   executed, and the counter should be only incremented once.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));

  // Sleep for another 3 seconds. The counter should remain 1. This verifies
  // the original scheduled task (3 seconds) is canceled.
  std::this_thread::sleep_for(std::chrono::milliseconds(3000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));
}

TEST(SimpleTaskSchedulerTest, ScheduleAt_overwriteTask) {
  Clock clock;
  int counter = 0;
  const auto task1 = [&counter]() { ++counter; };
  const auto task2 = [&counter]() { counter += 10; };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire in 3 seconds.
  scheduler->ScheduleAt(kTaskId, task1,
                        clock.GetSystemTimeMilliseconds() + 3000);

  // Sleep for 100 ms and reschedule the task to fire in 1 second, with a
  // different task implementation.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  scheduler->ScheduleAt(kTaskId, task2,
                        clock.GetSystemTimeMilliseconds() + 1000);

  // - After 1 second, the task should fire.
  // - It should execute the 2nd task, so the counter is incremented by 10.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(10));

  // Sleep for another 3 seconds. The counter should remain 10. This verifies
  // the original scheduled task (3 seconds) is not executed.
  std::this_thread::sleep_for(std::chrono::milliseconds(3000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(10));
}

TEST(SimpleTaskSchedulerTest, ScheduleAt_negativeTimestampShouldCancel) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() { ++counter; };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire in 1 second.
  scheduler->ScheduleAt(kTaskId, task,
                        clock.GetSystemTimeMilliseconds() + 1000);

  // Schedule again with a negative timestamp to cancel the previous task. The
  // scheduled time should be updated to -1, which means the task is canceled.
  scheduler->ScheduleAt(kTaskId, task, -100);
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId), Eq(-1));

  // The task should NOT fire.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(0));
}

TEST(SimpleTaskSchedulerTest,
     ScheduleAt_timestampEarlierThanNowShouldFireImmediately) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() { ++counter; };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task with a timestamp earlier than now.
  scheduler->ScheduleAt(kTaskId, task,
                        clock.GetSystemTimeMilliseconds() - 1000);

  // The task should fire immediately and increment the counter.
  std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));
}

TEST(SimpleTaskSchedulerTest, Cancel) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() { ++counter; };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire in 1 second.
  scheduler->ScheduleAt(kTaskId, task,
                        clock.GetSystemTimeMilliseconds() + 1000);

  // Cancel the task after 100 ms.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  scheduler->Cancel(kTaskId);

  // - After 1 second, the task should not fire and the counter should remain 0.
  // - Although Cancel() interrupts wait_for(), the task should not be executed,
  //   and the counter should not be incremented.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(0));
}

TEST(SimpleTaskSchedulerTest, GetScheduledTimeMs) {
  FakeClock fake_clock;
  fake_clock.SetSystemTimeMilliseconds(0);
  const auto task = []() {
    // no-op
  };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(fake_clock);

  // Before scheduling, the scheduled time should be -1 (invalid timestamp).
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId), Eq(-1));

  scheduler->ScheduleAt(kTaskId, task, 100000);
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId), Eq(100000));

  scheduler->ScheduleAt(kTaskId, task, 200000);
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId), Eq(200000));

  scheduler->ScheduleAt(kTaskId, task, 300000);
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId), Eq(300000));

  scheduler->Cancel(kTaskId);
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId), Eq(-1));

  scheduler->ScheduleAt(kTaskId, task, 400000);
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId), Eq(400000));

  scheduler->ScheduleAt(kTaskId, task, -1000);
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId), Eq(-1));

  // Unregistered task id should return -1.
  EXPECT_THAT(scheduler->GetScheduledTimeMs(kTaskId + 1), Eq(-1));
}

TEST(SimpleTaskSchedulerTest, Destructor) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() { ++counter; };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire in 1 second.
  scheduler->ScheduleAt(kTaskId, task,
                        clock.GetSystemTimeMilliseconds() + 1000);

  // Destruct the scheduler after 100 ms.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  scheduler.reset();

  // After 1 second, the task should not fire and the counter should remain 0.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(0));
}

TEST(SimpleTaskSchedulerTest, Destructor_shouldCancelAndJoinThread) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() { ++counter; };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire in 1 second.
  scheduler->ScheduleAt(kTaskId, task,
                        clock.GetSystemTimeMilliseconds() + 1000);

  // Destruct the scheduler object immediately. The background thread should be
  // joined successfully in the destructor, without any crash or deadlock.
  scheduler.reset();

  EXPECT_THAT(counter, Eq(0));
}

TEST(SimpleTaskSchedulerTest, Destructor_noScheduledTask) {
  Clock clock;
  int counter = 0;

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Destruct the scheduler object immediately without scheduling any task.
  // There should be no crash or deadlock.
  scheduler.reset();

  EXPECT_THAT(counter, Eq(0));
}

TEST(SimpleTaskSchedulerTest, Destructor_noDeadlock) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() {
    ++counter;
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  };

  // Create a scheduler and schedule an expensive task that sleeps for 3
  // seconds.
  //
  // This allows the test to trigger a different execution order (race
  // condition) and verify the correctness of the conditional variable
  // notification in the destructor before joining the thread, and
  // is_terminated_ flag check.
  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire in 1 second.
  scheduler->ScheduleAt(kTaskId, task,
                        clock.GetSystemTimeMilliseconds() + 1000);

  // Sleep for 1 second.
  // - At this moment, the executor thread is still running the expensive task.
  // - Destruct the scheduler object at this moment.
  //   - The destructor will set is_terminated_ to true and notify the executor
  //     thread to terminate, but the executor thread is still running the
  //     expensive task, so the notification is no-op.
  //   - The destructor will join the executor thread.
  //   - After the executor thread finishes the expensive task, it should not
  //     call wait() or wait_for() again. Instead, it should terminate and join
  //     the destructor.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  scheduler.reset();

  EXPECT_THAT(counter, Eq(1));
}

TEST(SimpleTaskSchedulerTest, MultipleScheduledTasks) {
  Clock clock;
  std::atomic<int> counter = 0;

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  SimpleTaskScheduler::TaskId task_id1 = 1;
  SimpleTaskScheduler::TaskId task_id2 = 2;
  // Schedule task_id1 to fire in 2 seconds.
  int64_t current_time_ms = clock.GetSystemTimeMilliseconds();
  scheduler->ScheduleAt(
      task_id1, [&counter]() { ++counter; }, current_time_ms + 2000);
  // Schedule task_id2 to fire in 1 second.
  scheduler->ScheduleAt(
      task_id2, [&counter]() { counter += 10; }, current_time_ms + 1000);

  // After 1 second, task_id2 should fire and increment the counter by 10.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(10));

  // After another 1 second, task_id1 should fire and increment the counter
  // by 1.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(11));
}

TEST(SimpleTaskSchedulerTest, ExpensiveTask_nextExecutionShouldWaitAndExecute) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() {
    ++counter;
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  };

  // Create a scheduler and schedule an expensive task that sleeps for 2
  // seconds.
  //
  // This allows the test to trigger a different execution order (race
  // condition) and verify the correctness of the conditional variable
  // notification in the destructor before joining the thread, and
  // is_terminated_ flag check.
  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire immediately. The counter is incremented, but the
  // 1st execution is still running.
  scheduler->ScheduleAt(kTaskId, task, clock.GetSystemTimeMilliseconds());
  std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));

  // Schedule another task to fire in 10 ms for the 2nd execution.
  scheduler->ScheduleAt(kTaskId, task, clock.GetSystemTimeMilliseconds() + 10);
  // Sleep for 10 ms. The 1st execution is still running, so the 2nd execution
  // should wait and run after the 1st execution finishes.
  std::this_thread::sleep_for(std::chrono::milliseconds(10 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));

  // Finally, the 1st execution finishes and the 2nd execution fires.
  std::this_thread::sleep_for(std::chrono::milliseconds(2000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(2));
}

TEST(SimpleTaskSchedulerTest,
     ExpensiveTask_multipleSchedulesDuringOngoingExecutionFiresOnceAfterwards) {
  Clock clock;
  int counter = 0;
  const auto task = [&counter]() {
    ++counter;
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  };

  // Create a scheduler and schedule an expensive task that sleeps for 2
  // seconds.
  //
  // This allows the test to trigger a different execution order (race
  // condition) and verify the correctness of the conditional variable
  // notification in the destructor before joining the thread, and
  // is_terminated_ flag check.
  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(clock);

  // Schedule a task to fire immediately. The counter is incremented, but the
  // 1st execution is still running.
  scheduler->ScheduleAt(kTaskId, task, clock.GetSystemTimeMilliseconds());
  std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));

  // Schedule the task for the 2nd time to fire in 10 ms.
  scheduler->ScheduleAt(kTaskId, task, clock.GetSystemTimeMilliseconds() + 10);
  // Sleep for 10 ms. The 1st execution is still running, so the next execution
  // should wait and execute after the 1st execution finishes.
  std::this_thread::sleep_for(std::chrono::milliseconds(10 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));

  // Schedule the task for the 3rd time to fire in 20 ms.
  scheduler->ScheduleAt(kTaskId, task, clock.GetSystemTimeMilliseconds() + 20);
  // Sleep for 10 ms. The 1st execution is still running, so the next execution
  // should wait and execute after the 1st execution finishes.
  std::this_thread::sleep_for(std::chrono::milliseconds(10 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));

  // Finally, the 1st execution finishes and another execution fires.
  std::this_thread::sleep_for(std::chrono::milliseconds(2000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(2));

  // Sleep for another 2 seconds. The counter should remain 2 since only one
  // execution should fire after the 1st execution finishes, even though there
  // were two executions expected to fire when the 1st execution was still
  // running.
  std::this_thread::sleep_for(std::chrono::milliseconds(2000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(2));
}

TEST(SimpleTaskSchedulerTest, MockClock) {
  FakeClock fake_clock;
  int counter = 0;
  const auto task = [&counter]() { ++counter; };

  std::unique_ptr<SimpleTaskScheduler> scheduler =
      SimpleTaskScheduler::Create(fake_clock);

  fake_clock.SetSystemTimeMilliseconds(30000);
  // Schedule a task to fire in 1 second.
  scheduler->ScheduleAt(kTaskId, task,
                        fake_clock.GetSystemTimeMilliseconds() + 1000);

  // After 1 second, the task should fire and increment the counter.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(counter, Eq(1));
}

TEST(SimpleTaskSchedulerTest, SelfTask) {
  Clock clock;

  auto main_class = std::make_unique<MainClass>(clock);
  main_class->Initialize();

  // After 1 second, the task should fire and increment the counter.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(main_class->GetCounter(), Eq(1));

  // After another 1 second, the task should fire and increment the counter.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 + kSleepMsEps));
  EXPECT_THAT(main_class->GetCounter(), Eq(2));
}

}  // namespace

}  // namespace lib
}  // namespace icing
