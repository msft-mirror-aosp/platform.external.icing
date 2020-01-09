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

#ifndef ICING_ABSL_PORTS_STATUS_MACROS_H_
#define ICING_ABSL_PORTS_STATUS_MACROS_H_

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"

namespace icing {
namespace lib {
namespace absl_ports {

// TODO(b/144458732): Move the fixes included in this file over to TC Status and
// remove this file.
class StatusAdapter {
 public:
  explicit StatusAdapter(const libtextclassifier3::Status& s) : s_(s) {}
  explicit StatusAdapter(libtextclassifier3::Status&& s) : s_(std::move(s)) {}
  template <typename T>
  explicit StatusAdapter(const libtextclassifier3::StatusOr<T>& s)
      : s_(s.status()) {}
  template <typename T>
  explicit StatusAdapter(libtextclassifier3::StatusOr<T>&& s)
      : s_(std::move(s).status()) {}

  bool ok() const { return s_.ok(); }
  explicit operator bool() const { return ok(); }

  const libtextclassifier3::Status& status() const& { return s_; }
  libtextclassifier3::Status status() && { return std::move(s_); }

 private:
  libtextclassifier3::Status s_;
};

}  // namespace absl_ports
}  // namespace lib
}  // namespace icing

// Evaluates an expression that produces a `libtextclassifier3::Status`. If the
// status is not ok, returns it from the current function.
//
// For example:
//   libtextclassifier3::Status MultiStepFunction() {
//     ICING_RETURN_IF_ERROR(Function(args...));
//     ICING_RETURN_IF_ERROR(foo.Method(args...));
//     return libtextclassifier3::Status();
//   }
#define ICING_RETURN_IF_ERROR(expr) ICING_RETURN_IF_ERROR_IMPL(expr)
#define ICING_RETURN_IF_ERROR_IMPL(expr)                       \
  ICING_STATUS_MACROS_IMPL_ELSE_BLOCKER_                       \
  if (::icing::lib::absl_ports::StatusAdapter adapter{expr}) { \
  } else /* NOLINT */                                          \
    return std::move(adapter).status()

// The GNU compiler emits a warning for code like:
//
//   if (foo)
//     if (bar) { } else baz;
//
// because it thinks you might want the else to bind to the first if.  This
// leads to problems with code like:
//
//   if (do_expr) ICING_RETURN_IF_ERROR(expr);
//
// The "switch (0) case 0:" idiom is used to suppress this.
#define ICING_STATUS_MACROS_IMPL_ELSE_BLOCKER_ \
  switch (0)                                   \
  case 0:                                      \
  default:  // NOLINT

#endif  // ICING_ABSL_PORTS_STATUS_MACROS_H_
