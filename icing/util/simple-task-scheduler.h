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

#ifndef ICING_UTIL_SIMPLE_TASK_SCHEDULER_H_
#define ICING_UTIL_SIMPLE_TASK_SCHEDULER_H_

#include <condition_variable>  // NOLINT
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <unordered_map>
#include <utility>

#include "icing/util/clock.h"

namespace icing {
namespace lib {

// A utility class to schedule custom tasks to fire at a specific timestamp.
//
// Example:
//   Clock clock;
//   std::unique_ptr<SimpleTaskScheduler> scheduler =
//       SimpleTaskScheduler::Create(clock);
//
//   // Schedule task_id1 to fire in 5 seconds.
//   SimpleTaskScheduler::TaskId task_id1 = 1;
//   scheduler->ScheduleAt(
//       task_id1,
//       [&]() { FooApi(); },
//       clock.GetSystemTimeMilliseconds() + 5000);
//
//   // Schedule task_id2 to fire in 1 second.
//   SimpleTaskScheduler::TaskId task_id2 = 2;
//   scheduler->ScheduleAt(
//       task_id2,
//       [&]() { BarApi(); },
//       clock.GetSystemTimeMilliseconds() + 1000);
//
// Then, task_id2 will fire first (running BarApi()), and then task_id1 will
// fire afterwards (running FooApi()).
//
// Note: tasks are executed sequentially and there is only one execution at a
// time.
// - Tasks may be blocked by other requests to Icing.
// - A task scheduled for time T will execute at the greater of T or the
//   earliest time at which no previously scheduled tasks or incoming api calls
//   are running.
class SimpleTaskScheduler {
 public:
  using TaskId = int32_t;

  // Creates a SimpleTaskScheduler.
  //
  // Note: after creation, no task is scheduled yet. It is only active after
  //   calling ScheduleAt().
  static std::unique_ptr<SimpleTaskScheduler> Create(const Clock& clock);

  ~SimpleTaskScheduler();

  // Disables copy, move, and assignment.
  SimpleTaskScheduler(const SimpleTaskScheduler&) = delete;
  SimpleTaskScheduler& operator=(const SimpleTaskScheduler&) = delete;
  SimpleTaskScheduler(SimpleTaskScheduler&&) = delete;
  SimpleTaskScheduler& operator=(SimpleTaskScheduler&&) = delete;

  // (Re)Schedules the task (given the task id) with the given new timestamp in
  // milliseconds.
  // - If there is a pending task of the same id, then its execution time will
  //   be overwritten by new_execution_timestamp_ms and the pending task is
  //   rescheduled. The task function is also overwritten.
  // - If new_execution_timestamp_ms is not greater than the current time
  //   (clock_.GetSystemTimeMilliseconds()), an execution will fire without
  //   waiting (as long as there is no ongoing execution).
  // - If the timestamp is invalid (smaller than 0), the previous scheduled task
  //   of this id will be canceled.
  // - If it is time to run another new scheduled task but there is still an
  //   ongoing execution, then the new scheduled task will wait until the
  //   ongoing execution finishes and fire after that.
  void ScheduleAt(TaskId task_id, std::function<void()> task,
                  int64_t new_execution_timestamp_ms);

  // Cancels the task by the task id.
  // - It only cancels the pending task and does not interrupt an ongoing
  //   (already started) execution.
  // - If no task was scheduled or the task id is invalid, then this is a no-op.
  void Cancel(TaskId task_id);

  // Returns the scheduled time (next execution time) of the task in
  // milliseconds. If the task id is unregistered or there is no scheduled
  // execution, then returns -1.
  int64_t GetScheduledTimeMs(TaskId task_id) const;

 private:
  static constexpr int64_t kInvalidTimestampMs = -1;

  struct TaskInfo {
    std::shared_ptr<std::function<void()>> task;
    int64_t execution_timestamp_ms;

    explicit TaskInfo(std::function<void()> task_in,
                      int64_t execution_timestamp_ms_in)
        : task(std::make_shared<std::function<void()>>(std::move(task_in))),
          execution_timestamp_ms(execution_timestamp_ms_in) {}
  };

  explicit SimpleTaskScheduler(const Clock& clock)
      : clock_(clock), is_terminated_(false) {};

  // Helper function to initialize the scheduler.
  void Initialize();

  // Gets the next task info to execute.
  //
  // Returns:
  //   - nullptr if there is no scheduled task.
  //   - Otherwise, a valid TaskInfo pointer for the next scheduled task. i.e.
  //     the task info with the smallest valid execution timestamp of all
  //     scheduled tasks.
  SimpleTaskScheduler::TaskInfo* GetNextTaskInfoLocked() const;

  // Helper function to clean up completed or canceled tasks (i.e. tasks with
  // invalid execution timestamp).
  //
  // Note: this function MUST be called only by the executor thread.
  void CleanUpCompletedTasksLocked();

  // The executor thread for waiting for the execution time and executing the
  // task.
  //
  // Use unique_ptr to delay initialization the thread.
  std::unique_ptr<std::thread> thread_;

  // Mutex and condition variable for the thread to wait and be notified.
  mutable std::mutex mutex_;  // NOLINT
  std::condition_variable cv_;

  // The clock used to get the current time.
  const Clock& clock_;  // Does not own.

  // Shared memory between the main thread and the executor thread.
  // - The main thread will use public methods to update the values and notify
  //   the executor thread via cv_.
  // - The executor thread will use the values to determine the next action
  //   after being notified.

  // Mapping scheduled task id to info.
  std::unordered_map<TaskId, std::unique_ptr<TaskInfo>> tasks_;

  // Whether the scheduler is about to be destroyed or not.
  bool is_terminated_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_SIMPLE_TASK_SCHEDULER_H_
