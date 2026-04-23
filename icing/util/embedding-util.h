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

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/util/crc32.h"
#include "icing/util/encode-util.h"

namespace icing {
namespace lib {

namespace embedding_util {

// The maximum length returned by encode_util::EncodeIntToCString is 5 for
// uint32_t.
inline constexpr uint32_t kEncodedDimensionLength = 5;

inline uint32_t GetPostingListKeyHash(std::string_view posting_list_key) {
  return Crc32(posting_list_key).Get();
}

// An injective function that maps the ordered pair (dimension, model_signature)
// to a string, which is used to form a key for embedding_posting_list_mapper_.
inline std::string GetPostingListKey(uint32_t dimension,
                                     std::string_view model_signature) {
  std::string encoded_dimension_str =
      encode_util::EncodeIntToCString(dimension);
  // Make encoded_dimension_str to fixed kEncodedDimensionLength bytes.
  while (encoded_dimension_str.size() < kEncodedDimensionLength) {
    // C string cannot contain 0 bytes, so we append it using 1, just like what
    // we do in encode_util::EncodeIntToCString.
    //
    // The reason that this works is because DecodeIntToString decodes a byte
    // value of 0x01 as 0x00. When EncodeIntToCString returns an encoded
    // dimension that is less than 5 bytes, it means that the dimension contains
    // unencoded leading 0x00. So here we're explicitly encoding those bytes as
    // 0x01.
    encoded_dimension_str.push_back(1);
  }
  return absl_ports::StrCat(encoded_dimension_str, model_signature);
}

inline std::string GetPostingListKey(const PropertyProto::VectorProto& vector) {
  return GetPostingListKey(vector.values().size(), vector.model_signature());
}

inline constexpr std::string_view kIvfPostingListKeySeparator = "\xFE\xFF";

// Constants for cluster IDs
inline constexpr uint32_t kLinearSearchClusterId = 0;
inline constexpr uint32_t kIvfCentroidsClusterId = 1;
inline constexpr uint32_t kIvfDeltaStoreClusterId = 2;
// Real clusters start from this cluster id.
inline constexpr uint32_t kIvfBaseClusterId = 3;

struct ParsedPostingListKey {
  std::string base_key;
  uint32_t dimension = 0;
  uint32_t cluster_id = 0;
};

inline uint32_t GetDimensionFromPostingListKey(std::string_view key) {
  return encode_util::DecodeIntFromCString(
      std::string_view(key.begin(), kEncodedDimensionLength));
}

inline libtextclassifier3::StatusOr<ParsedPostingListKey> ParsePostingListKey(
    std::string_view key) {
  ParsedPostingListKey result;
  if (key.size() < kEncodedDimensionLength) {
    return absl_ports::InternalError("Invalid posting list key");
  }
  result.dimension = GetDimensionFromPostingListKey(key);
  size_t separator_pos = key.rfind(kIvfPostingListKeySeparator);
  if (separator_pos != std::string_view::npos &&
      separator_pos >= kEncodedDimensionLength) {
    result.base_key = std::string(key.substr(0, separator_pos));
    std::string_view cluster_id_str =
        key.substr(separator_pos + kIvfPostingListKeySeparator.size());
    if (cluster_id_str.empty()) {
      return absl_ports::InternalError("Invalid IVF posting list key");
    }
    result.cluster_id = encode_util::DecodeIntFromCString(cluster_id_str);
  } else {
    result.base_key = std::string(key);
    result.cluster_id = kLinearSearchClusterId;
  }
  return result;
}

// The list of embedding query metric types, with the order matching the
// enum value.
static const std::array<SearchSpecProto::EmbeddingQueryMetricType::Code, 3>
    kEmbeddingQueryMetricTypes = {
        SearchSpecProto::EmbeddingQueryMetricType::COSINE,       // value = 1
        SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,  // value = 2
        SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN     // value = 3
};

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
