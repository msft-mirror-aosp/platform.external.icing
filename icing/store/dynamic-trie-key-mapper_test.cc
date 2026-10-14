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

#include "icing/store/dynamic-trie-key-mapper.h"

#include <limits>
#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;
using ::testing::Gt;

constexpr int kMaxDynamicTrieKeyMapperSize = 3 * 1024 * 1024;  // 3 MiB

class DynamicTrieKeyMapperTest : public testing::Test {
 protected:
  void SetUp() override { base_dir_ = GetTestTempDir() + "/key_mapper"; }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  std::string base_dir_;
  Filesystem filesystem_;
};

TEST_F(DynamicTrieKeyMapperTest, InvalidBaseDir) {
  EXPECT_THAT(DynamicTrieKeyMapper<DocumentId>::Create(
                  filesystem_, "/dev/null", kMaxDynamicTrieKeyMapperSize),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(DynamicTrieKeyMapperTest, NegativeMaxKeyMapperSizeReturnsInternalError) {
  EXPECT_THAT(
      DynamicTrieKeyMapper<DocumentId>::Create(filesystem_, base_dir_, -1),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(DynamicTrieKeyMapperTest, TooLargeMaxKeyMapperSizeReturnsInternalError) {
  EXPECT_THAT(DynamicTrieKeyMapper<DocumentId>::Create(
                  filesystem_, base_dir_, std::numeric_limits<int>::max()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(DynamicTrieKeyMapperTest, GetOrPutExistingKeyWhenFullShouldSucceed) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<DocumentId>> key_mapper,
      DynamicTrieKeyMapper<DocumentId>::Create(
          filesystem_, base_dir_,
          /*maximum_size_bytes=*/3 * 128 * 1024));

  // Insert long keys until the trie is full.
  const std::string long_suffix(1000, 'a');
  int num_keys = 0;
  libtextclassifier3::Status put_status;
  for (int i = 0; i < 10000 && put_status.ok(); ++i) {
    put_status =
        key_mapper
            ->GetOrPut(absl_ports::StrCat(std::to_string(i), long_suffix), i)
            .status();
    if (put_status.ok()) {
      ++num_keys;
    }
  }
  ASSERT_THAT(put_status,
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  ASSERT_THAT(num_keys, Gt(1));

  // GetOrPut on existing keys should still return their values.
  EXPECT_THAT(
      key_mapper->GetOrPut(absl_ports::StrCat("0", long_suffix), num_keys),
      IsOkAndHolds(0));
  EXPECT_THAT(key_mapper->GetOrPut(
                  absl_ports::StrCat(std::to_string(num_keys - 1), long_suffix),
                  num_keys),
              IsOkAndHolds(num_keys - 1));
  EXPECT_THAT(key_mapper->num_keys(), Eq(num_keys));
}

}  // namespace

}  // namespace lib
}  // namespace icing
