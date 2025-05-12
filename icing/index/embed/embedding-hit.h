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

#ifndef ICING_INDEX_EMBED_EMBEDDING_HIT_H_
#define ICING_INDEX_EMBED_EMBEDDING_HIT_H_

#include <cstdint>

#include "icing/index/hit/hit.h"

namespace icing {
namespace lib {

class EmbeddingHit {
 public:
  // Order: basic_hit, location
  // Value bits layout: 32 basic_hit + 32 location
  using Value = uint64_t;

  // WARNING: Changing this value will invalidate any pre-existing posting lists
  // on user devices.
  //
  // kInvalidValue contains:
  // - BasicHit of value 0, which is invalid.
  // - location of 0 (valid), which is ok because BasicHit is already invalid.
  static_assert(BasicHit::kInvalidValue == 0);
  static constexpr Value kInvalidValue = 0;

  explicit EmbeddingHit(BasicHit basic_hit, uint32_t location) {
    value_ = (static_cast<uint64_t>(basic_hit.value()) << 32) | location;
  }

  explicit EmbeddingHit(Value value) : value_(value) {}

  // BasicHit contains the document id and the section id.
  BasicHit basic_hit() const { return BasicHit(value_ >> 32); }
  // The location of the referred embedding vector in the vector storage.
  uint32_t location() const { return value_ & 0xFFFFFFFF; };

  bool is_valid() const { return basic_hit().is_valid(); }
  Value value() const { return value_; }

  bool operator<(const EmbeddingHit& h2) const { return value_ < h2.value_; }
  bool operator==(const EmbeddingHit& h2) const { return value_ == h2.value_; }

 private:
  Value value_;
};

static_assert(sizeof(BasicHit) == 4);
static_assert(sizeof(EmbeddingHit) == 8);

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_HIT_H_
