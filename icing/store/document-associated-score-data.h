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

#ifndef ICING_STORE_DOCUMENT_ASSOCIATED_SCORE_DATA_H_
#define ICING_STORE_DOCUMENT_ASSOCIATED_SCORE_DATA_H_

#include <cstdint>
#include <type_traits>

#include "icing/legacy/core/icing-packed-pod.h"

namespace icing {
namespace lib {

// This is the cache entity of document-associated scores. It contains scores
// that are related to the document itself. The ground-truth data is stored
// somewhere else. The cache includes:
// 1. Document score. It's defined in and passed from DocumentProto.score.
//    Positive values are required.
// 2. Document creation timestamp. Unix timestamp of when the document is
//    created and inserted into Icing.
class DocumentAssociatedScoreData {
 public:
  explicit DocumentAssociatedScoreData(int document_score,
                                       int64_t creation_timestamp_secs)
      : document_score_(document_score),
        creation_timestamp_secs_(creation_timestamp_secs) {}

  bool operator==(const DocumentAssociatedScoreData& other) const {
    return document_score_ == other.document_score() &&
           creation_timestamp_secs_ == other.creation_timestamp_secs();
  }

  int document_score() const { return document_score_; }

  int64_t creation_timestamp_secs() const { return creation_timestamp_secs_; }

 private:
  int document_score_;
  int64_t creation_timestamp_secs_;
} __attribute__((packed));

static_assert(sizeof(DocumentAssociatedScoreData) == 12,
              "Size of DocumentAssociatedScoreData should be 12");
static_assert(icing_is_packed_pod<DocumentAssociatedScoreData>::value,
              "go/icing-ubsan");

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_DOCUMENT_ASSOCIATED_SCORE_DATA_H_
