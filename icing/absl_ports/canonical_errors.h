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

#ifndef ICING_ABSL_PORTS_CANONICAL_ERRORS_H_
#define ICING_ABSL_PORTS_CANONICAL_ERRORS_H_

#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"

namespace icing {
namespace lib {
namespace absl_ports {

// Overload both const char* and std::string to support 2 different callers:
// `FooError(StrCat("text", a_str, " and ", b_str));` and
// `FooError("simple text");`
//
// - std::string is used for the first call to avoid additional copies (Note:
//   if using std::string_view, then the caller cannot move the string).
// - const char* is used for the second call.
//   - If const char* is not overloaded, then it is still valid for the second
//     call to use std::string.
//   - However, in Android the compiler will implicitly compile some conversion
//     code (from const char* to std::string) **on the caller side** and
//     increase most of the classes' size by ~1 KB, and the size of libicing.so
//     will be increased by ~100 KBs.
//   - From the experiment, overloading both const char* and std::string can
//     avoid this binary size increase.
libtextclassifier3::Status CancelledError(const char* error_message);
libtextclassifier3::Status UnknownError(const char* error_message);
libtextclassifier3::Status InvalidArgumentError(const char* error_message);
libtextclassifier3::Status DeadlineExceededError(const char* error_message);
libtextclassifier3::Status NotFoundError(const char* error_message);
libtextclassifier3::Status AlreadyExistsError(const char* error_message);
libtextclassifier3::Status PermissionDeniedError(const char* error_message);
libtextclassifier3::Status ResourceExhaustedError(const char* error_message);
libtextclassifier3::Status FailedPreconditionError(const char* error_message);
libtextclassifier3::Status AbortedError(const char* error_message);
libtextclassifier3::Status OutOfRangeError(const char* error_message);
libtextclassifier3::Status UnimplementedError(const char* error_message);
libtextclassifier3::Status InternalError(const char* error_message);
libtextclassifier3::Status UnavailableError(const char* error_message);
libtextclassifier3::Status DataLossError(const char* error_message);
libtextclassifier3::Status UnauthenticatedError(const char* error_message);

libtextclassifier3::Status CancelledError(std::string error_message);
libtextclassifier3::Status UnknownError(std::string error_message);
libtextclassifier3::Status InvalidArgumentError(std::string error_message);
libtextclassifier3::Status DeadlineExceededError(std::string error_message);
libtextclassifier3::Status NotFoundError(std::string error_message);
libtextclassifier3::Status AlreadyExistsError(std::string error_message);
libtextclassifier3::Status PermissionDeniedError(std::string error_message);
libtextclassifier3::Status ResourceExhaustedError(std::string error_message);
libtextclassifier3::Status FailedPreconditionError(std::string error_message);
libtextclassifier3::Status AbortedError(std::string error_message);
libtextclassifier3::Status OutOfRangeError(std::string error_message);
libtextclassifier3::Status UnimplementedError(std::string error_message);
libtextclassifier3::Status InternalError(std::string error_message);
libtextclassifier3::Status UnavailableError(std::string error_message);
libtextclassifier3::Status DataLossError(std::string error_message);
libtextclassifier3::Status UnauthenticatedError(std::string error_message);

bool IsCancelled(const libtextclassifier3::Status& status);
bool IsUnknown(const libtextclassifier3::Status& status);
bool IsInvalidArgument(const libtextclassifier3::Status& status);
bool IsDeadlineExceeded(const libtextclassifier3::Status& status);
bool IsNotFound(const libtextclassifier3::Status& status);
bool IsAlreadyExists(const libtextclassifier3::Status& status);
bool IsPermissionDenied(const libtextclassifier3::Status& status);
bool IsResourceExhausted(const libtextclassifier3::Status& status);
bool IsFailedPrecondition(const libtextclassifier3::Status& status);
bool IsAborted(const libtextclassifier3::Status& status);
bool IsOutOfRange(const libtextclassifier3::Status& status);
bool IsUnimplemented(const libtextclassifier3::Status& status);
bool IsInternal(const libtextclassifier3::Status& status);
bool IsUnavailable(const libtextclassifier3::Status& status);
bool IsDataLoss(const libtextclassifier3::Status& status);
bool IsUnauthenticated(const libtextclassifier3::Status& status);

}  // namespace absl_ports
}  // namespace lib
}  // namespace icing

#endif  // ICING_ABSL_PORTS_CANONICAL_ERRORS_H_
