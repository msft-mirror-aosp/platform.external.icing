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

#include "icing/util/sha256.h"

#include <array>
#include <cstdint>
#include <cstring>

namespace icing {
namespace lib {

// Constants for SHA-256 algorithm
constexpr uint32_t k[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

constexpr uint8_t kPaddingFirstByte = 0x80;
constexpr uint8_t kNullChar = '\0';

// Function to perform a right rotation on a 32-bit value
uint32_t RightRotate(uint32_t value, unsigned int count) {
  return value >> count | value << (32 - count);
}

Sha256::Sha256() {
  state_[0] = 0x6a09e667;
  state_[1] = 0xbb67ae85;
  state_[2] = 0x3c6ef372;
  state_[3] = 0xa54ff53a;
  state_[4] = 0x510e527f;
  state_[5] = 0x9b05688c;
  state_[6] = 0x1f83d9ab;
  state_[7] = 0x5be0cd19;
  count_ = 0;
  memset(buffer_.data(), 0, sizeof(buffer_));
}

void Sha256::Transform() {
  uint32_t w[64];
  int t = 0;
  // Process the first 16 words of the message block
  for (; t < 16; ++t) {
      uint32_t tmp = static_cast<uint32_t>(buffer_[t * 4]) << 24;
      tmp |= static_cast<uint32_t>(buffer_[t * 4 + 1]) << 16;
      tmp |= static_cast<uint32_t>(buffer_[t * 4 + 2]) << 8;
      tmp |= static_cast<uint32_t>(buffer_[t * 4 + 3]);
      w[t] = tmp;
  }

  // Extend the first 16 words into the remaining 48 words of the message
  // schedule
  for (; t < 64; t++) {
    // Calculate the next word in the message schedule based on the previous
    // words
    uint32_t s0 = RightRotate(w[t - 15], 7) ^ RightRotate(w[t - 15], 18) ^
                  (w[t - 15] >> 3);
    uint32_t s1 = RightRotate(w[t - 2], 17) ^ RightRotate(w[t - 2], 19) ^
                  (w[t - 2] >> 10);
    w[t] = w[t - 16] + s0 + w[t - 7] + s1;
  }

  uint32_t a = state_[0];
  uint32_t b = state_[1];
  uint32_t c = state_[2];
  uint32_t d = state_[3];
  uint32_t e = state_[4];
  uint32_t f = state_[5];
  uint32_t g = state_[6];
  uint32_t h = state_[7];

  for (int i = 0; i < 64; i++) {
    uint32_t sigma0 =
        RightRotate(a, 2) ^ RightRotate(a, 13) ^ RightRotate(a, 22);
    uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
    uint32_t temp2 = sigma0 + majority;
    uint32_t sigma1 =
        RightRotate(e, 6) ^ RightRotate(e, 11) ^ RightRotate(e, 25);
    uint32_t choice = (e & f) ^ ((~e) & g);
    uint32_t temp1 = h + sigma1 + choice + k[i] + w[i];

    h = g;
    g = f;
    f = e;
    e = d + temp1;
    d = c;
    c = b;
    b = a;
    a = temp1 + temp2;
  }

  state_[0] += a;
  state_[1] += b;
  state_[2] += c;
  state_[3] += d;
  state_[4] += e;
  state_[5] += f;
  state_[6] += g;
  state_[7] += h;
}

void Sha256::Update(const uint8_t* data, size_t length) {
  int i = static_cast<int>(count_ & 0b111111);
  count_ += length;
  while (length--) {
    buffer_[i] = *data;
    ++data;
    ++i;
    if (i == 64) {
      Transform();
      i = 0;
    }
  }
}

std::array<uint8_t, 32> Sha256::Finalize() && {
  uint64_t bits_count = count_ << 3;

  // SHA-256 padding: the message is padded with a '1' bit, then with '0' bits
  // until the message length modulo 512 is 448 bits (56 bytes modulo 64).
  Update(&kPaddingFirstByte, 1);

  // Pad with '0' bits until the message length modulo 64 is 56 bytes, leaving
  // 8 bytes for the length of the original message.
  while (count_ % 64 != 56) {
    Update(&kNullChar, 1);
  }

  // Append the length of the original message (in bits) to the end of the
  // padded message in big-endian order.
  for (int i = 0; i < 8; ++i) {
    uint8_t tmp = static_cast<uint8_t>(bits_count >> 56);
    bits_count <<= 8;
    Update(&tmp, 1);
  }

  // Convert the state array to a 32-byte sha256 hash array in big-endian order.
  std::array<uint8_t, 32> hash;
  for (int i = 0, j = 0; i < 8; i++) {
    uint32_t tmp = state_[i];
    hash[j++] = static_cast<uint8_t>(tmp >> 24);
    hash[j++] = static_cast<uint8_t>(tmp >> 16);
    hash[j++] = static_cast<uint8_t>(tmp >> 8);
    hash[j++] = static_cast<uint8_t>(tmp);
  }

  return hash;
}

}  // namespace lib
}  // namespace icing
