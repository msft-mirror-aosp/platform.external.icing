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

#ifndef ICING_STORE_DOCUMENT_FILTER_DATA_H_
#define ICING_STORE_DOCUMENT_FILTER_DATA_H_

#include <cstdint>

#include "icing/legacy/core/icing-packed-pod.h"
#include "icing/store/namespace-id.h"

namespace icing {
namespace lib {

using SchemaTypeId = int16_t;
inline constexpr SchemaTypeId kInvalidSchemaTypeId = -1;

class DocumentFilterData {
 public:
  explicit DocumentFilterData(NamespaceId namespace_id,
                              SchemaTypeId schema_type_id,
                              int64_t expiration_timestamp_ms,
                              int64_t raw_expiration_timestamp_ms)
      : expiration_timestamp_ms_(expiration_timestamp_ms),
        raw_expiration_timestamp_ms_(raw_expiration_timestamp_ms),
        namespace_id_(namespace_id),
        schema_type_id_(schema_type_id) {}

  bool operator==(const DocumentFilterData& other) const {
    return namespace_id_ == other.namespace_id() &&
           schema_type_id_ == other.schema_type_id() &&
           expiration_timestamp_ms_ == other.expiration_timestamp_ms() &&
           raw_expiration_timestamp_ms_ == other.raw_expiration_timestamp_ms();
  }

  NamespaceId namespace_id() const { return namespace_id_; }

  SchemaTypeId schema_type_id() const { return schema_type_id_; }
  void set_schema_type_id(SchemaTypeId schema_type_id) {
    schema_type_id_ = schema_type_id;
  }

  int64_t expiration_timestamp_ms() const { return expiration_timestamp_ms_; }

  int64_t raw_expiration_timestamp_ms() const {
    return raw_expiration_timestamp_ms_;
  }

 private:
  // The expiration timestamp of the document in milliseconds. This is the min
  // value of:
  // - The document's raw expiration timestamp (see
  //   raw_expiration_timestamp_ms_).
  // - The propagated expiration timestamps according to its dependencies
  //   specified in join properties with delete propagation enabled.
  //
  // Icing will use this value to:
  // - Determine if a document is alive or expired.
  // - Propagte to its dependents' expiration timestamps. Note that in most
  //   cases the propagation should use this value instead of the raw expiration
  //   timestamp.
  int64_t expiration_timestamp_ms_;

  // The raw expiration timestamp of the document in milliseconds, calculated by
  // the document's creation timestamp and ttl. This value is cached and mostly
  // should only be used to reset expiration_timestamp_ms_ when Icing needs to
  // recompute the propagated expiration timestamps of all documents.
  int64_t raw_expiration_timestamp_ms_;

  NamespaceId namespace_id_;
  SchemaTypeId schema_type_id_;
} __attribute__((packed));

static_assert(sizeof(DocumentFilterData) == 20, "");
static_assert(icing_is_packed_pod<DocumentFilterData>::value, "go/icing-ubsan");

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_DOCUMENT_FILTER_DATA_H_
