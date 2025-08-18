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

// EqualsProto is only available in google3 version of protobuf for Messages.
// This compat-header provides a simple EqualsProto matcher for MessageLite
// in the non-google3 version of protobufs, and includes the existing
// EqualsProto matcher in the google3 version, based on the build flags.

#ifndef ICING_PORTABLE_EQUALS_PROTO_H_
#define ICING_PORTABLE_EQUALS_PROTO_H_

#include "gmock/gmock.h"          // IWYU pragma: export
#include <google/protobuf/message_lite.h>  // IWYU pragma: export

namespace icing {
namespace lib {
namespace portable_equals_proto {
// We need this matcher because MessageLite does not support reflection.
// Hence, there is no better way to compare two protos of an arbitrary type.
// This matcher enables comparing non-google3 protos on, e.g., Android, with
// a known caveat that it is unable to provide detailed difference information.
#if defined(__ANDROID__) || defined(__APPLE__)

MATCHER_P(EqualsProto, other, "Compare MessageLite by serialized string") {
  return ::testing::ExplainMatchResult(::testing::Eq(other.SerializeAsString()),
                                       arg.SerializeAsString(),
                                       result_listener);
}

MATCHER(EqualsProto, "") {
  return ::testing::ExplainMatchResult(EqualsProto(std::get<1>(arg)),
                                       std::get<0>(arg), result_listener);
}

#else

// Leverage the powerful google3 matcher when available, for human readable
// differences.
using ::testing::EqualsProto;

#endif  // defined(__ANDROID__) || defined(__APPLE__)

}  // namespace portable_equals_proto
}  // namespace lib
}  // namespace icing

#endif  // ICING_PORTABLE_EQUALS_PROTO_H_
