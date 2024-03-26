// Copyright (C) 2024 Google LLC
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

#ifndef ICING_UTIL_EMBEDDING_UTIL_H_
#define ICING_UTIL_EMBEDDING_UTIL_H_

#include <string_view>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/proto/search.pb.h"

namespace icing {
namespace lib {

namespace embedding_util {

inline libtextclassifier3::StatusOr<
    SearchSpecProto::EmbeddingQueryMetricType::Code>
GetEmbeddingQueryMetricTypeFromName(std::string_view metric_name) {
  if (metric_name == "COSINE") {
    return SearchSpecProto::EmbeddingQueryMetricType::COSINE;
  } else if (metric_name == "DOT_PRODUCT") {
    return SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT;
  } else if (metric_name == "EUCLIDEAN") {
    return SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN;
  }
  return absl_ports::InvalidArgumentError(
      absl_ports::StrCat("Unknown metric type: ", metric_name));
}

}  // namespace embedding_util

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_EMBEDDING_UTIL_H_
