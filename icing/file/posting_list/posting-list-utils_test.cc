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

#include "icing/file/posting_list/posting-list-utils.h"

#include <cstdint>

#include "gtest/gtest.h"

namespace icing {
namespace lib {
namespace posting_list_utils {
namespace {

TEST(PostingListUtilsTest, IsValidPostingListSize_Valid) {
  EXPECT_TRUE(IsValidPostingListSize(/*size_in_bytes=*/16,
                                     /*data_type_bytes=*/4,
                                     /*min_posting_list_size=*/4));
}

TEST(PostingListUtilsTest, IsValidPostingListSize_WastedSpaceAlign) {
  EXPECT_FALSE(IsValidPostingListSize(/*size_in_bytes=*/15,
                                      /*data_type_bytes=*/4,
                                      /*min_posting_list_size=*/4));
}

TEST(PostingListUtilsTest, IsValidPostingListSize_TooSmall) {
  EXPECT_FALSE(IsValidPostingListSize(/*size_in_bytes=*/8,
                                      /*data_type_bytes=*/4,
                                      /*min_posting_list_size=*/12));
}

TEST(PostingListUtilsTest, IsValidPostingListSize_TooLargeForOffset) {
  // If data_type_bytes is 2 (16 bits) and size_in_bytes is 65538 (requires 17
  // bits), then BitsToStore(65538) is 17, which is > 16.
  EXPECT_FALSE(IsValidPostingListSize(/*size_in_bytes=*/65538,
                                      /*data_type_bytes=*/2,
                                      /*min_posting_list_size=*/2));
}

}  // namespace
}  // namespace posting_list_utils
}  // namespace lib
}  // namespace icing
