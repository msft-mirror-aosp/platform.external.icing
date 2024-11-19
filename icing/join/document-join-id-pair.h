// Copyright (C) 2023 Google LLC
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

#ifndef ICING_JOIN_DOCUMENT_JOIN_ID_PAIR_H_
#define ICING_JOIN_DOCUMENT_JOIN_ID_PAIR_H_

#include <cstdint>
#include <limits>

#include "icing/schema/joinable-property.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// DocumentJoinIdPair is composed of document_id and joinable_property_id.
class DocumentJoinIdPair {
 public:
  // The datatype used to encode DocumentJoinIdPair information: the document_id
  // and joinable_property_id.
  using Value = uint32_t;

  static_assert(kDocumentIdBits + kJoinablePropertyIdBits <= sizeof(Value) * 8,
                "Cannot encode document id and joinable property id in "
                "DocumentJoinIdPair::Value");

  // All bits of kInvalidValue are 1, and it contains:
  // - 0b1 for 4 unused bits.
  // - kInvalidDocumentId (2^22-1).
  // - JoinablePropertyId 2^6-1 (valid), which is ok because kInvalidDocumentId
  //   has already invalidated the value. In fact, we currently use all 2^6
  //   joinable property ids and there is no "invalid joinable property id", so
  //   it doesn't matter what JoinablePropertyId we set for kInvalidValue.
  static constexpr Value kInvalidValue = std::numeric_limits<Value>::max();

  // Default constexpr constructor to construct an invalid DocumentJoinIdPair.
  constexpr DocumentJoinIdPair() : value_(kInvalidValue) {}

  explicit DocumentJoinIdPair(DocumentId document_id,
                              JoinablePropertyId joinable_property_id);

  explicit DocumentJoinIdPair(Value value) : value_(value) {}

  bool operator==(const DocumentJoinIdPair& other) const {
    return value_ == other.value_;
  }

  bool is_valid() const { return value_ != kInvalidValue; }
  Value value() const { return value_; }
  DocumentId document_id() const;
  JoinablePropertyId joinable_property_id() const;

 private:
  // Value bits layout: 4 unused + 22 document_id + 6 joinable_property_id.
  Value value_;
} __attribute__((packed));
static_assert(sizeof(DocumentJoinIdPair) == 4, "");

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_DOCUMENT_JOIN_ID_PAIR_H_
