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

#ifndef ICING_UTIL_SHA256_H_
#define ICING_UTIL_SHA256_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>

namespace icing {
namespace lib {

using Sha256Digest = std::array<uint8_t, 32>;
const size_t kSha256DigestBytes = 32;

class Sha256 {
 public:
  Sha256();

  // Update the SHA256 context with additional data
  void Update(const uint8_t* data, size_t length);

  // Finalize the SHA256 computation and obtain the 32-byte hash.
  Sha256Digest Finalize() &&;

 private:
  // Array to hold the current hash state
  uint32_t state_[8];

  // Total number of bytes processed
  uint64_t count_;

  // The 64-byte buffer to store the input data, sha-256 block size is 64 bytes.
  std::array<uint8_t, 64> buffer_;

  // Processes a block of input data and updates the hash state.
  void Transform();
};

}  // namespace lib
}  // namespace icing

namespace std {

template <>
struct hash<icing::lib::Sha256Digest> {
  // FNV-1a constants for 64-bits. Please see
  // https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
  // for more details.
  static constexpr uint64_t FNV_PRIME = 0x100000001b3ULL;
  static constexpr uint64_t FNV_OFFSET_BASIS = 0xcbf29ce484222325ULL;

  uint64_t operator()(const icing::lib::Sha256Digest& digest) const {
    uint64_t hash = FNV_OFFSET_BASIS;

    // Process all 32 bytes using FNV-1a
    for (uint8_t byte : digest) {
      hash ^= static_cast<uint64_t>(byte);
      hash *= FNV_PRIME;
    }

    return hash;
  }
};

}  // namespace std

#endif  // ICING_UTIL_SHA256_H_
