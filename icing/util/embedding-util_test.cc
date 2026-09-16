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

#include "icing/util/embedding-util.h"

#include <cstdint>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/testing/common-matchers.h"
#include "icing/util/encode-util.h"

namespace icing {
namespace lib {
namespace {

using ::testing::HasSubstr;

TEST(EmbeddingUtilTest, ParseLinearSearchPostingListKey) {
  std::string base_key = embedding_util::GetPostingListKey(128, "model_sig");

  ICING_ASSERT_OK_AND_ASSIGN(embedding_util::ParsedPostingListKey parsed_key,
                             embedding_util::ParsePostingListKey(base_key));

  EXPECT_EQ(parsed_key.dimension, 128);
  EXPECT_EQ(parsed_key.base_key, base_key);
  EXPECT_EQ(parsed_key.cluster_id, embedding_util::kLinearSearchClusterId);
}

TEST(EmbeddingUtilTest, ParseIvfPostingListKey) {
  std::string base_key = embedding_util::GetPostingListKey(128, "model_sig");
  uint32_t cluster_id = 42;
  std::string ivf_key =
      absl_ports::StrCat(base_key, embedding_util::kIvfPostingListKeySeparator,
                         encode_util::EncodeIntToCString(cluster_id));

  ICING_ASSERT_OK_AND_ASSIGN(embedding_util::ParsedPostingListKey parsed_key,
                             embedding_util::ParsePostingListKey(ivf_key));

  EXPECT_EQ(parsed_key.dimension, 128);
  EXPECT_EQ(parsed_key.base_key, base_key);
  EXPECT_EQ(parsed_key.cluster_id, cluster_id);
}

TEST(EmbeddingUtilTest, ParsePostingListKeyInvalidLength) {
  EXPECT_THAT(embedding_util::ParsePostingListKey(""),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL,
                       HasSubstr("Invalid posting list key")));
}

TEST(EmbeddingUtilTest, ParseIvfPostingListKeyEmptyClusterId) {
  std::string base_key = embedding_util::GetPostingListKey(128, "model_sig");
  std::string ivf_key =
      absl_ports::StrCat(base_key, embedding_util::kIvfPostingListKeySeparator);

  EXPECT_THAT(embedding_util::ParsePostingListKey(ivf_key),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL,
                       HasSubstr("Invalid IVF posting list key")));
}

}  // namespace
}  // namespace lib
}  // namespace icing
