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

#ifndef ICING_INDEX_TERM_ID_HIT_PAIR_H_
#define ICING_INDEX_TERM_ID_HIT_PAIR_H_

#include <array>
#include <cstdint>

#include "icing/index/hit/hit.h"

namespace icing {
namespace lib {

class TermIdHitPair {
 public:
  // Layout bits: 24 termid + 32 hit value + 8 hit flags + 8 hit term frequency.
  using Value = std::array<uint8_t, 9>;

  static constexpr int kTermIdBits = 24;
  static constexpr int kHitValueBits = sizeof(Hit::Value) * 8;
  static constexpr int kHitFlagsBits = sizeof(Hit::Flags) * 8;
  static constexpr int kHitTermFrequencyBits = sizeof(Hit::TermFrequency) * 8;

  static const Value kInvalidValue;

  explicit TermIdHitPair(Value v = kInvalidValue) : value_(v) {}

  TermIdHitPair(uint32_t term_id, const Hit& hit) {
    static_assert(
        kTermIdBits + kHitValueBits + kHitFlagsBits + kHitTermFrequencyBits <=
            sizeof(Value) * 8,
        "TermIdHitPairTooBig");

    // Set termId. Term id takes 3 bytes and goes into value_[0:2] (most
    // significant bits) because it takes precedent in sorts.
    value_[0] = static_cast<uint8_t>((term_id >> 16) & 0xff);
    value_[1] = static_cast<uint8_t>((term_id >> 8) & 0xff);
    value_[2] = static_cast<uint8_t>((term_id >> 0) & 0xff);

    // Set hit value. Hit value takes 4 bytes and goes into value_[3:6]
    value_[3] = static_cast<uint8_t>((hit.value() >> 24) & 0xff);
    value_[4] = static_cast<uint8_t>((hit.value() >> 16) & 0xff);
    value_[5] = static_cast<uint8_t>((hit.value() >> 8) & 0xff);
    value_[6] = static_cast<uint8_t>((hit.value() >> 0) & 0xff);

    // Set flags in value_[7].
    value_[7] = hit.flags();

    // Set term-frequency in value_[8]
    value_[8] = hit.term_frequency();
  }

  uint32_t term_id() const {
    return (static_cast<uint32_t>(value_[0]) << 16) |
           (static_cast<uint32_t>(value_[1]) << 8) |
           (static_cast<uint32_t>(value_[2]) << 0);
  }

  Hit hit() const {
    Hit::Value hit_value = (static_cast<uint32_t>(value_[3]) << 24) |
                           (static_cast<uint32_t>(value_[4]) << 16) |
                           (static_cast<uint32_t>(value_[5]) << 8) |
                           (static_cast<uint32_t>(value_[6]) << 0);
    Hit::Flags hit_flags = value_[7];
    Hit::TermFrequency term_frequency = value_[8];

    return Hit(hit_value, hit_flags, term_frequency);
  }

  const Value& value() const { return value_; }

  bool operator==(const TermIdHitPair& rhs) const {
    return value_ == rhs.value_;
  }

  bool operator<(const TermIdHitPair& rhs) const { return value_ < rhs.value_; }

 private:
  Value value_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_TERM_ID_HIT_PAIR_H_
