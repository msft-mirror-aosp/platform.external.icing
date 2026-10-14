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

#include <chrono>              // NOLINT
#include <condition_variable>  // NOLINT
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <utility>

#include "icing/util/clock.h"

namespace icing {
namespace lib {

std::unique_ptr<SimpleTaskScheduler> SimpleTaskScheduler::Create(
    const Clock& clock) {
  auto scheduler =
      std::unique_ptr<SimpleTaskScheduler>(new SimpleTaskScheduler(clock));
  scheduler->Initialize();
  return scheduler;
}

SimpleTaskScheduler::~SimpleTaskScheduler() {
  // Notify the thread to terminate.
  {
    std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

    is_terminated_ = true;
  }
  cv_.notify_all();

  if (thread_ != nullptr) {
    thread_->join();
  }
}

void SimpleTaskScheduler::ScheduleAt(TaskId task_id, std::function<void()> task,
                                     int64_t new_execution_timestamp_ms) {
  if (new_execution_timestamp_ms < 0) {
    new_execution_timestamp_ms = kInvalidTimestampMs;
  }

  // - Check if the task id exists.
  //   - If yes, then overwrite the TaskInfo for the task id.
  //   - If no, then create a new task info and add it into the map.
  // - Notify the executor thread blocking at wait() or wait_for() to wake up
  //   and wait for the new execution time.
  {
    std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

    auto itr = tasks_.find(task_id);
    if (itr != tasks_.end()) {
      itr->second->task =
          std::make_shared<std::function<void()>>(std::move(task));
      itr->second->execution_timestamp_ms = new_execution_timestamp_ms;
    } else {
      tasks_.insert(
          {task_id, std::make_unique<TaskInfo>(std::move(task),
                                               new_execution_timestamp_ms)});
    }
  }
  cv_.notify_all();
}

void SimpleTaskScheduler::Cancel(TaskId task_id) {
  {
    std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

    auto itr = tasks_.find(task_id);
    if (itr == tasks_.end()) {
      return;
    }

    // Set the timestamp to kInvalidTimestampMs to cancel the task.
    //
    // Note: we CANNOT remove the entry from the map here. Suppose the executor
    //   thread is currently waiting for the execution of THIS task (i.e.
    //   blocking at wait_for()):
    // - We need kInvalidTimestampMs as a mark to tell the executor thread not
    //   to execute the task after waking up.
    // - Also in this case, the executor thread currently holds a TaskInfo
    //   pointer to THIS task in a local variable. Removing the task entry from
    //   the map here will cause pointer instability and crash after waking up.
    //
    // Therefore, we keep the entry in the map, and delegate the "cleanup" work
    // to the executor thread.
    itr->second->execution_timestamp_ms = kInvalidTimestampMs;
  }
  cv_.notify_all();
}

int64_t SimpleTaskScheduler::GetScheduledTimeMs(TaskId task_id) const {
  std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

  auto itr = tasks_.find(task_id);
  if (itr != tasks_.end()) {
    return itr->second->execution_timestamp_ms;
  }
  return kInvalidTimestampMs;
}

void SimpleTaskScheduler::Initialize() {
  std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

  thread_ = std::make_unique<std::thread>([this]() {
    while (true) {
      std::shared_ptr<std::function<void()>> next_task = nullptr;

      {
        std::unique_lock<std::mutex> lk(mutex_);  // NOLINT

        if (is_terminated_) {
          break;
        }

        CleanUpCompletedTasksLocked();

        int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
        TaskInfo* next_task_info = GetNextTaskInfoLocked();
        if (next_task_info == nullptr) {
          // No scheduled task. Wait for the next task being scheduled. This
          // will avoid busy-waiting.
          cv_.wait(lk);
        } else if (next_task_info->execution_timestamp_ms > current_time_ms) {
          // Wait for the execution time. It is possible that wait_for() is
          // interrupted by ScheduleAt(), Cancel(), or spurious wakeup.
          //
          // Note: we use wait_for() instead of wait_until() for the convenience
          //   of mocking the clock in the unit test.
          std::cv_status wait_status = cv_.wait_for(
              lk,
              std::chrono::milliseconds(next_task_info->execution_timestamp_ms -
                                        current_time_ms));
          // If the thread is woken up by timeout, execute the task.
          // Otherwise, the thread is woken up by ScheduleAt(), Cancel() or
          // spurious wakeup.
          if (wait_status != std::cv_status::timeout) {
            // Set the pointer to nullptr indicating that we need another round
            // of wait() before executing it.
            next_task_info = nullptr;
          }
        }

        // It is possible that is_terminated_ is modified during wait() or
        // wait_for(). Check it again here.
        if (is_terminated_) {
          break;
        }

        // It is possible that the task is canceled or rescheduled during
        // wait_for().
        // - In most cases, wait_status above will NOT be
        //   std::cv_status::timeout and we should've already set next_task_info
        //   to nullptr.
        // - However, it is possible that the cancel/reschedule operation took
        //   place right at the wait_for() timeout. Therefore, let's check
        //   execution_timestamp_ms again here.
        if (next_task_info != nullptr &&
            next_task_info->execution_timestamp_ms != kInvalidTimestampMs) {
          // Reset the timestamp before executing the task.
          next_task_info->execution_timestamp_ms = kInvalidTimestampMs;

          // Get a shared pointer to the task.
          //
          // Note: since we execute the task outside of the critical section,
          //   it is possible that the task is overwritten between the gap of
          //   the critical section and the actual execution below (i.e. the
          //   caller calls ScheduleAt() with the same task id + a different
          //   task implementation and overwrites the task object). Here, we
          //   have to execute the original task, so we need to use a shared
          //   pointer for it.
          next_task = next_task_info->task;
        }
      }

      if (next_task != nullptr) {
        // It is possible that the task is expensive or attempts to acquire
        // the mutex again and schedule another task, so execute the current one
        // outside of the critical section to avoid blocking or deadlock.
        (*next_task)();
      }
    }
  });
}

SimpleTaskScheduler::TaskInfo* SimpleTaskScheduler::GetNextTaskInfoLocked()
    const {
  TaskInfo* next_task_info = nullptr;
  for (const auto& [_, task_info] : tasks_) {
    if (task_info->execution_timestamp_ms == kInvalidTimestampMs) {
      continue;
    }

    if (next_task_info == nullptr ||
        task_info->execution_timestamp_ms <
            next_task_info->execution_timestamp_ms) {
      next_task_info = task_info.get();
    }
  }
  return next_task_info;
}

void SimpleTaskScheduler::CleanUpCompletedTasksLocked() {
  for (auto itr = tasks_.begin(); itr != tasks_.end();) {
    if (itr->second->execution_timestamp_ms == kInvalidTimestampMs) {
      itr = tasks_.erase(itr);
    } else {
      ++itr;
    }
  }
}

}  // namespace lib
}  // namespace icing
