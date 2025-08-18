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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace icing {
namespace lib {
namespace encode_util {

namespace {

using ::testing::Eq;
using ::testing::Gt;
using ::testing::SizeIs;

TEST(EncodeUtilTest, IntCStringZeroConversion) {
  uint64_t value = 0;
  std::string encoded_str = EncodeIntToCString(value);

  EXPECT_THAT(encoded_str, SizeIs(Gt(0)));
  EXPECT_THAT(DecodeIntFromCString(encoded_str), Eq(value));
}

TEST(EncodeUtilTest, IntCStringConversionIsReversible) {
  uint64_t value = 123456;
  std::string encoded_str = EncodeIntToCString(value);
  EXPECT_THAT(DecodeIntFromCString(encoded_str), Eq(value));
}

TEST(EncodeUtilTest, MultipleIntCStringConversionsAreReversible) {
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(25)), Eq(25));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(766)), Eq(766));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(2305)), Eq(2305));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(6922)), Eq(6922));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(62326)), Eq(62326));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(186985)), Eq(186985));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(560962)), Eq(560962));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(1682893)), Eq(1682893));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(15146065)), Eq(15146065));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(136314613)),
              Eq(136314613));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(1226831545)),
              Eq(1226831545));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(11041483933)),
              Eq(11041483933));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(2683080596566)),
              Eq(2683080596566));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(72443176107373)),
              Eq(72443176107373));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(1955965754899162)),
              Eq(1955965754899162));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(52811075382277465)),
              Eq(52811075382277465));
  EXPECT_THAT(DecodeIntFromCString(EncodeIntToCString(4277697105964474945)),
              Eq(4277697105964474945));
}

TEST(EncodeUtilTest, MultipleValidEncodedCStringIntConversionsAreReversible) {
  // Only valid encoded C string (no zero bytes, length is between 1 and 10) are
  // reversible.
  EXPECT_THAT(EncodeIntToCString(DecodeIntFromCString("foo")), Eq("foo"));
  EXPECT_THAT(EncodeIntToCString(DecodeIntFromCString("bar")), Eq("bar"));
  EXPECT_THAT(EncodeIntToCString(DecodeIntFromCString("baz")), Eq("baz"));
  EXPECT_THAT(EncodeIntToCString(DecodeIntFromCString("Icing")), Eq("Icing"));
  EXPECT_THAT(EncodeIntToCString(DecodeIntFromCString("Google")), Eq("Google"));
  EXPECT_THAT(EncodeIntToCString(DecodeIntFromCString("Youtube")),
              Eq("Youtube"));
}

TEST(EncodeUtilTest, EncodeStringToCString) {
  std::string digest;
  digest.push_back(0b00000000);  // '\0'
  digest.push_back(0b00000001);  // '\1'
  digest.push_back(0b00000010);  // '\2'
  digest.push_back(0b00000011);  // '\3'
  digest.push_back(0b00000100);  // '\4'
  digest.push_back(0b00000101);  // '\5'
  digest.push_back(0b00000110);  // '\6'
  digest.push_back(0b00000111);  // '\7'
  digest.push_back(0b00001000);  // '\8'
  digest.push_back(0b00001001);  // '\9'
  std::string encoded_digest;
  // 1 + first 7 bits from '\0'
  encoded_digest.push_back(0b10000000);
  // 1 + last 1 bit from '\0' + first 6 bits from '\1'
  encoded_digest.push_back(0b10000000);
  // 1 + last 2 bits from '\1' + first 5 bits from '\2'
  encoded_digest.push_back(0b10100000);
  // 1 + last 3 bits from '\2' + first 4 bits from '\3'
  encoded_digest.push_back(0b10100000);
  // 1 + last 4 bits from '\3' + first 3 bits from '\4'
  encoded_digest.push_back(0b10011000);
  // 1 + last 5 bits from '\4' + first 2 bits from '\5'
  encoded_digest.push_back(0b10010000);
  // 1 + last 6 bits from '\5' + first 1 bits from '\6'
  encoded_digest.push_back(0b10001010);
  // 1 + last 7 bits from '\6'
  encoded_digest.push_back(0b10000110);
  // 1 + first 7 bits from '\7'
  encoded_digest.push_back(0b10000011);
  // 1 + last 1 bit from '\7' + first 6 bits from '\8'
  encoded_digest.push_back(0b11000010);
  // 1 + last 2 bit from '\8' + first 5 bits from '\9'
  encoded_digest.push_back(0b10000001);
  // 1 + last 3 bit from '\9' + filled with 0s
  encoded_digest.push_back(0b10010000);

  EXPECT_THAT(EncodeStringToCString(digest), Eq(encoded_digest));
}

TEST(EncodeUtilTest, EncodeEmptyStringToCString) {
  std::string digest;
  std::string encoded_digest;

  EXPECT_THAT(EncodeStringToCString(digest), Eq(encoded_digest));
}

TEST(EncodeUtilTest, EncodeMiddle0ByteStringToCStringConversions) {
  std::string digest;
  digest.push_back(0b00000001);  // '\1'
  digest.push_back(0b00000010);  // '\2'
  digest.push_back(0b00000011);  // '\3'
  digest.push_back(0b00000000);  // '\0'
  digest.push_back(0b00000100);  // '\4'
  digest.push_back(0b00000101);  // '\5'
  digest.push_back(0b00000110);  // '\6'
  std::string encoded_digest;
  // 1 + first 7 bits from '\1'
  encoded_digest.push_back(0b10000000);
  // 1 + last 1 bit from '\1' + first 6 bits from '\2'
  encoded_digest.push_back(0b11000000);
  // 1 + last 2 bits from '\2' + first 5 bits from '\3'
  encoded_digest.push_back(0b11000000);
  // 1 + last 3 bits from '\3' + first 4 bits from '\0'
  encoded_digest.push_back(0b10110000);
  // 1 + last 4 bits from '\0' + first 3 bits from '\4'
  encoded_digest.push_back(0b10000000);
  // 1 + last 5 bits from '\4' + first 2 bits from '\5'
  encoded_digest.push_back(0b10010000);
  // 1 + last 6 bits from '\5' + first 1 bits from '\6'
  encoded_digest.push_back(0b10001010);
  // 1 + last 7 bits from '\6'
  encoded_digest.push_back(0b10000110);

  EXPECT_THAT(EncodeStringToCString(digest), Eq(encoded_digest));
}

TEST(EncodeUtilTest, Encode32BytesDigestToCStringLength) {
  std::string digest(32, 0b00000000);
  // 37 = ceil(32 / 7.0 * 8.0)
  EXPECT_THAT(EncodeStringToCString(digest).size(), Eq(37));
}

}  // namespace

}  // namespace encode_util
}  // namespace lib
}  // namespace icing
