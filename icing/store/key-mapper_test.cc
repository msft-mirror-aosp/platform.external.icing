// Copyright (C) 2022 Google LLC
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

#include "icing/store/key-mapper.h"

#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/file/filesystem.h"
#include "icing/store/document-id.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/store/persistent-hash-map-key-mapper.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

namespace icing {
namespace lib {

namespace {

constexpr int kMaxDynamicTrieKeyMapperSize = 3 * 1024 * 1024;  // 3 MiB

template <typename T>
class KeyMapperTest : public ::testing::Test {
 protected:
  using KeyMapperType = T;

  void SetUp() override { base_dir_ = GetTestTempDir() + "/key_mapper"; }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  template <typename UnknownKeyMapperType>
  libtextclassifier3::StatusOr<std::unique_ptr<KeyMapper<DocumentId>>>
  CreateKeyMapper() {
    return absl_ports::InvalidArgumentError("Unknown type");
  }

  template <>
  libtextclassifier3::StatusOr<std::unique_ptr<KeyMapper<DocumentId>>>
  CreateKeyMapper<DynamicTrieKeyMapper<DocumentId>>() {
    return DynamicTrieKeyMapper<DocumentId>::Create(
        filesystem_, base_dir_, kMaxDynamicTrieKeyMapperSize);
  }

  template <>
  libtextclassifier3::StatusOr<std::unique_ptr<KeyMapper<DocumentId>>>
  CreateKeyMapper<PersistentHashMapKeyMapper<DocumentId>>() {
    return PersistentHashMapKeyMapper<DocumentId>::Create(filesystem_,
                                                          base_dir_);
  }

  std::string base_dir_;
  Filesystem filesystem_;
};

using TestTypes = ::testing::Types<DynamicTrieKeyMapper<DocumentId>,
                                   PersistentHashMapKeyMapper<DocumentId>>;
TYPED_TEST_SUITE(KeyMapperTest, TestTypes);

std::unordered_map<std::string, DocumentId> GetAllKeyValuePairs(
    const KeyMapper<DocumentId>* key_mapper) {
  std::unordered_map<std::string, DocumentId> ret;

  std::unique_ptr<typename KeyMapper<DocumentId>::Iterator> itr =
      key_mapper->GetIterator();
  while (itr->Advance()) {
    ret.emplace(itr->GetKey(), itr->GetValue());
  }
  return ret;
}

TYPED_TEST(KeyMapperTest, CreateNewKeyMapper) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyMapper<DocumentId>> key_mapper,
                             this->template CreateKeyMapper<TypeParam>());
  EXPECT_THAT(key_mapper->num_keys(), 0);
}

TYPED_TEST(KeyMapperTest, CanUpdateSameKeyMultipleTimes) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyMapper<DocumentId>> key_mapper,
                             this->template CreateKeyMapper<TypeParam>());

  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 100));
  ICING_EXPECT_OK(key_mapper->Put("default-youtube.com", 50));

  EXPECT_THAT(key_mapper->Get("default-google.com"), IsOkAndHolds(100));

  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 200));
  EXPECT_THAT(key_mapper->Get("default-google.com"), IsOkAndHolds(200));
  EXPECT_THAT(key_mapper->num_keys(), 2);

  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 300));
  EXPECT_THAT(key_mapper->Get("default-google.com"), IsOkAndHolds(300));
  EXPECT_THAT(key_mapper->num_keys(), 2);
}

TYPED_TEST(KeyMapperTest, GetOrPutOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyMapper<DocumentId>> key_mapper,
                             this->template CreateKeyMapper<TypeParam>());

  EXPECT_THAT(key_mapper->Get("foo"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(key_mapper->GetOrPut("foo", 1), IsOkAndHolds(1));
  EXPECT_THAT(key_mapper->Get("foo"), IsOkAndHolds(1));
}

TYPED_TEST(KeyMapperTest, CanPersistToDiskRegularly) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyMapper<DocumentId>> key_mapper,
                             this->template CreateKeyMapper<TypeParam>());

  // Can persist an empty DynamicTrieKeyMapper.
  ICING_EXPECT_OK(key_mapper->PersistToDisk());
  EXPECT_THAT(key_mapper->num_keys(), 0);

  // Can persist the smallest DynamicTrieKeyMapper.
  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 100));
  ICING_EXPECT_OK(key_mapper->PersistToDisk());
  EXPECT_THAT(key_mapper->num_keys(), 1);
  EXPECT_THAT(key_mapper->Get("default-google.com"), IsOkAndHolds(100));

  // Can continue to add keys after PersistToDisk().
  ICING_EXPECT_OK(key_mapper->Put("default-youtube.com", 200));
  EXPECT_THAT(key_mapper->num_keys(), 2);
  EXPECT_THAT(key_mapper->Get("default-youtube.com"), IsOkAndHolds(200));

  // Can continue to update the same key after PersistToDisk().
  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 300));
  EXPECT_THAT(key_mapper->Get("default-google.com"), IsOkAndHolds(300));
  EXPECT_THAT(key_mapper->num_keys(), 2);
}

TYPED_TEST(KeyMapperTest, CanUseAcrossMultipleInstances) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyMapper<DocumentId>> key_mapper,
                             this->template CreateKeyMapper<TypeParam>());
  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 100));
  ICING_EXPECT_OK(key_mapper->PersistToDisk());

  key_mapper.reset();

  ICING_ASSERT_OK_AND_ASSIGN(key_mapper,
                             this->template CreateKeyMapper<TypeParam>());
  EXPECT_THAT(key_mapper->num_keys(), 1);
  EXPECT_THAT(key_mapper->Get("default-google.com"), IsOkAndHolds(100));

  // Can continue to read/write to the KeyMapper.
  ICING_EXPECT_OK(key_mapper->Put("default-youtube.com", 200));
  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 300));
  EXPECT_THAT(key_mapper->num_keys(), 2);
  EXPECT_THAT(key_mapper->Get("default-youtube.com"), IsOkAndHolds(200));
  EXPECT_THAT(key_mapper->Get("default-google.com"), IsOkAndHolds(300));
}

TYPED_TEST(KeyMapperTest, CanDeleteAndRestartKeyMapping) {
  // Can delete even if there's nothing there
  ICING_EXPECT_OK(
      TestFixture::KeyMapperType::Delete(this->filesystem_, this->base_dir_));

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyMapper<DocumentId>> key_mapper,
                             this->template CreateKeyMapper<TypeParam>());
  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 100));
  ICING_EXPECT_OK(key_mapper->PersistToDisk());
  ICING_EXPECT_OK(
      TestFixture::KeyMapperType::Delete(this->filesystem_, this->base_dir_));

  key_mapper.reset();
  ICING_ASSERT_OK_AND_ASSIGN(key_mapper,
                             this->template CreateKeyMapper<TypeParam>());
  EXPECT_THAT(key_mapper->num_keys(), 0);
  ICING_EXPECT_OK(key_mapper->Put("default-google.com", 100));
  EXPECT_THAT(key_mapper->num_keys(), 1);
}

TYPED_TEST(KeyMapperTest, Iterator) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyMapper<DocumentId>> key_mapper,
                             this->template CreateKeyMapper<TypeParam>());
  EXPECT_THAT(GetAllKeyValuePairs(key_mapper.get()), IsEmpty());

  ICING_EXPECT_OK(key_mapper->Put("foo", /*value=*/1));
  ICING_EXPECT_OK(key_mapper->Put("bar", /*value=*/2));
  EXPECT_THAT(GetAllKeyValuePairs(key_mapper.get()),
              UnorderedElementsAre(Pair("foo", 1), Pair("bar", 2)));

  ICING_EXPECT_OK(key_mapper->Put("baz", /*value=*/3));
  EXPECT_THAT(
      GetAllKeyValuePairs(key_mapper.get()),
      UnorderedElementsAre(Pair("foo", 1), Pair("bar", 2), Pair("baz", 3)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
