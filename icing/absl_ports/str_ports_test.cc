// Copyright (C) 2026 Google LLC
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

#include <string>
#include <string_view>
#include <vector>

#include "icing/absl_ports/str_cat.h"
#include "icing/absl_ports/str_join.h"
#include "gtest/gtest.h"

namespace icing {
namespace lib {
namespace absl_ports {
namespace {

TEST(StrCatTest, Basic) {
  EXPECT_EQ(StrCat("a", "b"), "ab");
  EXPECT_EQ(StrCat("a", "b", "c"), "abc");
  EXPECT_EQ(StrCat("a", "b", "c", "d"), "abcd");
}

TEST(StrCatTest, Pieces) {
  std::vector<std::string_view> pieces = {"a", "b", "c"};
  EXPECT_EQ(StrCatPieces(pieces), "abc");
}

TEST(StrAppendTest, Basic) {
  std::string dest = "init";
  StrAppend(&dest, "a");
  EXPECT_EQ(dest, "inita");
  StrAppend(&dest, "b", "c");
  EXPECT_EQ(dest, "initabc");
  StrAppend(&dest, "d", "e", "f");
  EXPECT_EQ(dest, "initabcdef");
}

TEST(StrJoinTest, Basic) {
  std::vector<std::string> parts = {"a", "b", "c"};
  EXPECT_EQ(StrJoin(parts, ","), "a,b,c");
}

}  // namespace
}  // namespace absl_ports
}  // namespace lib
}  // namespace icing
