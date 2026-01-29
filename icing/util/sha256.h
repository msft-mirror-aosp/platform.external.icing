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

namespace icing {
namespace lib {

class Sha256 {
 public:
  Sha256();
  
  // Update the SHA256 context with additional data
  void Update(const uint8_t* data, size_t length);

  // Finalize the SHA256 computation and obtain the 32-byte hash.
  std::array<uint8_t, 32> Finalize() &&;

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

#endif  // ICING_UTIL_SHA256_H_
