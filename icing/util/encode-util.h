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

#ifndef ICING_UTIL_ENCODE_UTIL_H_
#define ICING_UTIL_ENCODE_UTIL_H_

#include <cstdint>
#include <string>
#include <string_view>

namespace icing {
namespace lib {

namespace encode_util {

// Converts an unsigned 64-bit integer to a C string that doesn't contain 0-byte
// since C string uses 0-byte as terminator. This increases the size of the
// encoded_str from 8-bytes to 10-bytes at worst.
//
// Note that it is compatible with unsigned 32-bit integers, i.e. casting an
// uint32_t to uint64_t with the same value and encoding it by this method will
// get the same string.
std::string EncodeIntToCString(uint64_t value);

// Converts a C string (encoded from EncodeIntToCString()) to an unsigned 64-bit
// integer.
uint64_t DecodeIntFromCString(std::string_view encoded_str);

// Converts the given string which may contains 0-byte to a C string.
//
// The output C string that does not contain 0-byte since C string uses 0-byte
// as terminator.
// This will increase the size of the encoded_str.
// new_length = ceil(old_length / 7.0 * 8.0)
// Eg1: This increases the size from 32-bytes of sha-256 hash to 37-bytes C
// string.
// Eg2: This increases the size from 1-byte string to 2-bytes C string.
// Eg3: This increases the size from 2-byte string to 3-bytes C string.
std::string EncodeStringToCString(const std::string& input);

}  // namespace encode_util

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_ENCODE_UTIL_H_
