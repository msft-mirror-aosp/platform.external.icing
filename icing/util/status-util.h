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

#ifndef ICING_UTIL_STATUS_UTIL_H_
#define ICING_UTIL_STATUS_UTIL_H_

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/proto/status.pb.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

namespace status_util {

inline void TransformStatus(const libtextclassifier3::Status& internal_status,
                            StatusProto* status_proto) {
  StatusProto::Code code;
  if (!internal_status.ok()) {
    ICING_LOG(WARNING) << "Error: " << internal_status.error_code()
                       << ", Message: " << internal_status.error_message();
  }
  switch (internal_status.CanonicalCode()) {
    case libtextclassifier3::StatusCode::OK:
      code = StatusProto::OK;
      break;
    case libtextclassifier3::StatusCode::DATA_LOSS:
      code = StatusProto::WARNING_DATA_LOSS;
      break;
    case libtextclassifier3::StatusCode::INVALID_ARGUMENT:
      code = StatusProto::INVALID_ARGUMENT;
      break;
    case libtextclassifier3::StatusCode::NOT_FOUND:
      code = StatusProto::NOT_FOUND;
      break;
    case libtextclassifier3::StatusCode::FAILED_PRECONDITION:
      code = StatusProto::FAILED_PRECONDITION;
      break;
    case libtextclassifier3::StatusCode::ABORTED:
      code = StatusProto::ABORTED;
      break;
    case libtextclassifier3::StatusCode::INTERNAL:
      // TODO(b/147699081): Cleanup our internal use of INTERNAL since it
      // doesn't match with what it *should* indicate as described in
      // go/icing-library-apis.
      code = StatusProto::INTERNAL;
      break;
    case libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED:
      // TODO(b/147699081): Note that we don't detect all cases of OUT_OF_SPACE
      // (e.g. if the document log is full). And we use RESOURCE_EXHAUSTED
      // internally to indicate other resources are exhausted (e.g.
      // DocHitInfos) - although none of these are exposed through the API.
      // Consider separating the two cases out more clearly.
      code = StatusProto::OUT_OF_SPACE;
      break;
    case libtextclassifier3::StatusCode::ALREADY_EXISTS:
      code = StatusProto::ALREADY_EXISTS;
      break;
    case libtextclassifier3::StatusCode::CANCELLED:
      [[fallthrough]];
    case libtextclassifier3::StatusCode::UNKNOWN:
      [[fallthrough]];
    case libtextclassifier3::StatusCode::DEADLINE_EXCEEDED:
      [[fallthrough]];
    case libtextclassifier3::StatusCode::PERMISSION_DENIED:
      [[fallthrough]];
    case libtextclassifier3::StatusCode::OUT_OF_RANGE:
      [[fallthrough]];
    case libtextclassifier3::StatusCode::UNIMPLEMENTED:
      [[fallthrough]];
    case libtextclassifier3::StatusCode::UNAVAILABLE:
      [[fallthrough]];
    case libtextclassifier3::StatusCode::UNAUTHENTICATED:
      // Other internal status codes aren't supported externally yet. If it
      // should be supported, add another switch-case above.
      ICING_LOG(ERROR) << "Internal status code "
                       << internal_status.error_code()
                       << " not supported in the external API";
      code = StatusProto::UNKNOWN;
      break;
  }
  status_proto->set_code(code);
  status_proto->set_message(internal_status.error_message());
}

}  // namespace status_util

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_STATUS_UTIL_H_
