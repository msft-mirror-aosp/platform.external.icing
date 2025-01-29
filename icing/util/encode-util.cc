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

#include "icing/util/encode-util.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace icing {
namespace lib {

namespace encode_util {

std::string EncodeIntToCString(uint64_t value) {
  std::string encoded_str;
  // Encode it in base128 and add 1 to make sure that there is no 0-byte. This
  // increases the size of the encoded_str from 8-bytes to 10-bytes at worst.
  do {
    encoded_str.push_back((value & 0x7F) + 1);
    value >>= 7;
  } while (value);
  return encoded_str;
}

uint64_t DecodeIntFromCString(std::string_view encoded_str) {
  uint64_t value = 0;
  for (int i = encoded_str.length() - 1; i >= 0; --i) {
    value <<= 7;
    char c = encoded_str[i] - 1;
    value |= (c & 0x7F);
  }
  return value;
}

std::string EncodeStringToCString(const std::string& input) {
  std::string encoded_str;
  // use uint32_t to store 4 bytes, using unsigned types to keep automatically
  // remove extra left most bits.
  uint32_t bit_buffer = 0;
  int bit_count = 0;
  for (unsigned char c : input) {
    // Add the next byte to the buffer.
    bit_buffer = (bit_buffer << 8) | c;
    bit_count += 8;
    // Encode the first 7 bits of the byte buffer by adding 1 in front.
    while (bit_count >= 7) {
      bit_count -= 7;
      encoded_str += ((bit_buffer >> bit_count) & 0x7F) | 0x80;
    }
  }
  // Encode the remaining byte.
  if (bit_count > 0) {
    encoded_str += ((bit_buffer << (7 - bit_count)) & 0x7F) | 0x80;
  }
  return encoded_str;
}

}  // namespace encode_util

}  // namespace lib
}  // namespace icing
