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

#ifndef ICING_ABSL_PORTS_MUTEX_H_
#define ICING_ABSL_PORTS_MUTEX_H_

#include <mutex>         // NOLINT
#include <shared_mutex>  // NOLINT

#include "icing/absl_ports/thread_annotations.h"

namespace icing {
namespace lib {
namespace absl_ports {

// Simple wrapper around std::shared_mutex with annotations to allow thread
// annotation checks. It can act as a no-op mutex if is_noop is set to true.
class ICING_LOCKABLE shared_mutex {
 public:
  explicit shared_mutex(bool is_noop = false) : is_noop_(is_noop) {}

  void lock() ICING_EXCLUSIVE_LOCK_FUNCTION() {
    if (!is_noop_) m_.lock();
  }
  bool try_lock() ICING_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    return !is_noop_ ? m_.try_lock() : true;
  }
  void unlock() ICING_UNLOCK_FUNCTION() {
    if (!is_noop_) m_.unlock();
  }

  void lock_shared() ICING_SHARED_LOCK_FUNCTION() {
    if (!is_noop_) m_.lock_shared();
  }
  bool try_lock_shared() ICING_SHARED_TRYLOCK_FUNCTION(true) {
    return !is_noop_ ? m_.try_lock_shared() : true;
  }
  void unlock_shared() ICING_UNLOCK_FUNCTION() {
    if (!is_noop_) m_.unlock_shared();
  }

  bool is_noop() const { return is_noop_; }

 private:
  std::shared_mutex m_;
  bool is_noop_;
};

// Simple wrapper around std::unique_lock with annotations to allow thread
// annotation checks.
class ICING_SCOPED_LOCKABLE unique_lock {
 public:
  explicit unique_lock(shared_mutex* mu) ICING_EXCLUSIVE_LOCK_FUNCTION(mu)
      : lock_(*mu) {}
  ~unique_lock() ICING_UNLOCK_FUNCTION() = default;

  // For fine-grained locking if necessary.
  void unlock() ICING_UNLOCK_FUNCTION() { lock_.unlock(); }
  void lock() ICING_EXCLUSIVE_LOCK_FUNCTION() { lock_.lock(); }
  bool owns_lock() const { return lock_.owns_lock(); }

 private:
  std::unique_lock<shared_mutex> lock_;
};

// Simple wrapper around std::shared_lock with annotations to allow thread
// annotation checks.
class ICING_SCOPED_LOCKABLE shared_lock {
 public:
  explicit shared_lock(shared_mutex* mu) ICING_SHARED_LOCK_FUNCTION(mu)
      : lock_(*mu) {}
  ~shared_lock() ICING_UNLOCK_FUNCTION() = default;

  bool owns_lock() const { return lock_.owns_lock(); }

 private:
  std::shared_lock<shared_mutex> lock_;
};

}  // namespace absl_ports
}  // namespace lib
}  // namespace icing

#endif  // ICING_ABSL_PORTS_MUTEX_H_
