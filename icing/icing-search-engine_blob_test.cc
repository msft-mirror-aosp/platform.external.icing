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

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/jni/jni-cache.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/sha256.h"

namespace icing {
namespace lib {

namespace {

// For mocking purpose, we allow tests to provide a custom Filesystem.
class TestIcingSearchEngine : public IcingSearchEngine {
 public:
  TestIcingSearchEngine(const IcingSearchEngineOptions& options,
                        std::unique_ptr<const Filesystem> filesystem,
                        std::unique_ptr<const IcingFilesystem> icing_filesystem,
                        std::unique_ptr<Clock> clock,
                        std::unique_ptr<JniCache> jni_cache)
      : IcingSearchEngine(options, std::move(filesystem),
                          std::move(icing_filesystem), std::move(clock),
                          std::move(jni_cache)) {}
};

std::string GetTestBaseDir() { return GetTestTempDir() + "/icing"; }

// This test is meant to cover all tests relating to IcingSearchEngine::Delete*.
class IcingSearchEngineBlobTest : public testing::Test {
 protected:
  void SetUp() override {
    filesystem_.CreateDirectoryRecursively(GetTestBaseDir().c_str());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(GetTestBaseDir().c_str());
  }

  const Filesystem* filesystem() const { return &filesystem_; }

 private:
  Filesystem filesystem_;
};

// Non-zero value so we don't override it to be the current time
constexpr int64_t kDefaultCreationTimestampMs = 1575492852000;

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_enable_blob_store(true);
  return icing_options;
}

std::vector<unsigned char> GenerateRandomBytes(size_t length) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<unsigned char> distribution(0, 255);
  std::vector<unsigned char> random_bytes(length);
  for (size_t i = 0; i < length; ++i) {
    random_bytes[i] = distribution(gen);
  }
  return random_bytes;
}

std::array<uint8_t, 32> CalculateDigest(
    const std::vector<unsigned char>& data) {
  Sha256 sha256;
  sha256.Update(data.data(), data.size());
  std::array<uint8_t, 32> hash = std::move(sha256).Finalize();
  return hash;
}

SchemaProto CreateBlobSchema() {
  return SchemaBuilder()
      .AddType(SchemaTypeConfigBuilder()
                   .SetType("BlobType")
                   .AddProperty(PropertyConfigBuilder()
                                    .SetName("blob")
                                    .SetDataType(TYPE_BLOB_HANDLE)
                                    .SetCardinality(CARDINALITY_REQUIRED)))
      .Build();
}

DocumentProto CreateBlobDocument(std::string name_space, std::string uri,
                                 PropertyProto::BlobHandleProto blob_handle) {
  return DocumentBuilder()
      .SetKey(std::move(name_space), std::move(uri))
      .SetSchema("BlobType")
      .AddBlobHandleProperty("blob", blob_handle)
      .SetCreationTimestampMs(kDefaultCreationTimestampMs)
      .Build();
}

TEST_F(IcingSearchEngineBlobTest, InvalidBlobHandle) {
  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("blob");
  blob_handle.set_digest("invalid");

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  BlobProto write_blob_proto = icing.OpenWriteBlob(blob_handle);
  EXPECT_THAT(write_blob_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
  BlobProto commit_blob_proto = icing.CommitBlob(blob_handle);
  EXPECT_THAT(commit_blob_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
  BlobProto read_blob_proto = icing.OpenReadBlob(blob_handle);
  EXPECT_THAT(read_blob_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineBlobTest, BlobStoreDisabled) {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_enable_blob_store(false);

  IcingSearchEngine icing(icing_options, GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("blob");
  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  blob_handle.set_digest((void*)digest.data(), digest.size());

  BlobProto write_blob_proto = icing.OpenWriteBlob(blob_handle);
  EXPECT_THAT(write_blob_proto.status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  BlobProto commit_blob_proto = icing.CommitBlob(blob_handle);
  EXPECT_THAT(commit_blob_proto.status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  BlobProto read_blob_proto = icing.OpenReadBlob(blob_handle);
  EXPECT_THAT(read_blob_proto.status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
}

TEST_F(IcingSearchEngineBlobTest, WriteAndReadBlob) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("label");
  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  blob_handle.set_digest((void*)digest.data(), digest.size());

  BlobProto write_blob_proto = icing.OpenWriteBlob(blob_handle);
  ASSERT_THAT(write_blob_proto.status(), ProtoIsOk());
  {
    ScopedFd write_fd(write_blob_proto.file_descriptor());
    ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  }

  BlobProto commit_blob_proto = icing.CommitBlob(blob_handle);
  ASSERT_THAT(commit_blob_proto.status(), ProtoIsOk());

  BlobProto read_blob_proto = icing.OpenReadBlob(blob_handle);
  ASSERT_THAT(read_blob_proto.status(), ProtoIsOk());
  {
    ScopedFd read_fd(read_blob_proto.file_descriptor());

    uint64_t size = filesystem()->GetFileSize(*read_fd);
    std::unique_ptr<unsigned char[]> buf =
        std::make_unique<unsigned char[]>(size);
    EXPECT_TRUE(filesystem()->Read(read_fd.get(), buf.get(), size));
    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }
}

TEST_F(IcingSearchEngineBlobTest, WriteAndReadBlobByDocument) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("label");
  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  blob_handle.set_digest((void*)digest.data(), digest.size());

  BlobProto write_blob_proto = icing.OpenWriteBlob(blob_handle);
  ASSERT_THAT(write_blob_proto.status(), ProtoIsOk());

  {
    ScopedFd write_fd(write_blob_proto.file_descriptor());
    ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  }

  BlobProto commit_blob_proto = icing.CommitBlob(blob_handle);
  ASSERT_THAT(commit_blob_proto.status(), ProtoIsOk());

  // Set schema and put a document that contains the blob handle
  ASSERT_THAT(icing.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());
  ASSERT_THAT(
      icing.Put(CreateBlobDocument("namespace", "doc1", blob_handle)).status(),
      ProtoIsOk());

  // Read the document and its blob handle property.
  GetResultProto get_result =
      icing.Get("namespace", "doc1", GetResultSpecProto::default_instance());
  EXPECT_THAT(get_result.status(), ProtoIsOk());
  PropertyProto::BlobHandleProto out_blob_handle =
      get_result.document().properties().at(0).blob_handle_values().at(0);

  // use the output blob handle to read blob data.
  BlobProto read_blob_proto = icing.OpenReadBlob(out_blob_handle);
  ASSERT_THAT(read_blob_proto.status(), ProtoIsOk());
  {
    ScopedFd read_fd(read_blob_proto.file_descriptor());

    uint64_t size = filesystem()->GetFileSize(*read_fd);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    EXPECT_TRUE(filesystem()->Read(read_fd.get(), buf.get(), size));

    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }
}

TEST_F(IcingSearchEngineBlobTest, CommitDigestMisMatch) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("blob1");

  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  blob_handle.set_digest(std::string(digest.begin(), digest.end()));

  BlobProto write_blob_proto = icing.OpenWriteBlob(blob_handle);
  ASSERT_THAT(write_blob_proto.status(), ProtoIsOk());

  std::vector<unsigned char> data2 = GenerateRandomBytes(24);
  {
    ScopedFd write_fd(write_blob_proto.file_descriptor());
    ASSERT_TRUE(
        filesystem()->Write(write_fd.get(), data2.data(), data2.size()));
  }

  BlobProto commit_blob_proto = icing.CommitBlob(blob_handle);
  ASSERT_THAT(commit_blob_proto.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineBlobTest, ReadBlobWithoutPersistToDisk) {
  IcingSearchEngine icing1(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing1.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("blob1");

  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  blob_handle.set_digest((void*)digest.data(), digest.size());

  BlobProto write_blob_proto = icing1.OpenWriteBlob(blob_handle);
  ASSERT_THAT(write_blob_proto.status(), ProtoIsOk());

  {
    ScopedFd write_fd(write_blob_proto.file_descriptor());
    ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  }

  BlobProto commit_blob_proto = icing1.CommitBlob(blob_handle);
  ASSERT_THAT(commit_blob_proto.status(), ProtoIsOk());

  // Recreate icing, the blob info will be dropped since we haven't called
  // persistToDisk.
  IcingSearchEngine icing2(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing2.Initialize().status(), ProtoIsOk());

  BlobProto read_blob_proto = icing2.OpenReadBlob(blob_handle);
  EXPECT_THAT(read_blob_proto.status(), ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineBlobTest, ReadBlobWithPersistToDiskFull) {
  IcingSearchEngine icing1(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing1.Initialize().status(), ProtoIsOk());
  // set a schema to icing to avoid wipe out all directories.
  ASSERT_THAT(icing1.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("blob1");

  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  blob_handle.set_digest((void*)digest.data(), digest.size());

  BlobProto write_blob_proto = icing1.OpenWriteBlob(blob_handle);
  ASSERT_THAT(write_blob_proto.status(), ProtoIsOk());
  {
    ScopedFd write_fd(write_blob_proto.file_descriptor());
    ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  }
  BlobProto commit_blob_proto = icing1.CommitBlob(blob_handle);
  ASSERT_THAT(commit_blob_proto.status(), ProtoIsOk());

  EXPECT_THAT(icing1.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

  // Recreate icing, the blob info will be dropped since we haven't called
  // persistToDisk.
  IcingSearchEngine icing2(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  BlobProto read_blob_proto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(read_blob_proto.status(), ProtoIsOk());
  {
    ScopedFd read_fd(read_blob_proto.file_descriptor());
    uint64_t size = filesystem()->GetFileSize(*read_fd);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    EXPECT_TRUE(filesystem()->Read(read_fd.get(), buf.get(), size));
    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }
}

TEST_F(IcingSearchEngineBlobTest, ReadBlobWithPersistToDiskLite) {
  IcingSearchEngine icing1(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing1.Initialize().status(), ProtoIsOk());
  // set a schema to icing to avoid wipe out all directories.
  ASSERT_THAT(icing1.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("blob1");

  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  blob_handle.set_digest((void*)digest.data(), digest.size());

  BlobProto write_blob_proto = icing1.OpenWriteBlob(blob_handle);
  ASSERT_THAT(write_blob_proto.status(), ProtoIsOk());

  {
    ScopedFd write_fd(write_blob_proto.file_descriptor());
    ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  }

  BlobProto commit_blob_proto = icing1.CommitBlob(blob_handle);
  ASSERT_THAT(commit_blob_proto.status(), ProtoIsOk());

  EXPECT_THAT(icing1.PersistToDisk(PersistType::LITE).status(), ProtoIsOk());

  // Recreate icing, the blob info will be remained since we called
  // persistToDisk.
  IcingSearchEngine icing2(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  BlobProto read_blob_proto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(read_blob_proto.status(), ProtoIsOk());
  {
    ScopedFd read_fd(read_blob_proto.file_descriptor());
    uint64_t size = filesystem()->GetFileSize(*read_fd);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    EXPECT_TRUE(filesystem()->Read(read_fd.get(), buf.get(), size));
    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }
}

}  // namespace
}  // namespace lib
}  // namespace icing
