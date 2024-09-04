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
#include "icing/testing/fake-clock.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/sha256.h"

namespace icing {
namespace lib {

static constexpr int64_t kBlobInfoTTLMs = 7 * 24 * 60 * 60 * 1000;  // 1 Week

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
  icing_options.set_orphan_blob_time_to_live_ms(kBlobInfoTTLMs);
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

TEST_F(IcingSearchEngineBlobTest, BlobOptimize) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // set a schema to icing to avoid wipe out all directories.
  ASSERT_THAT(icing.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("label");
  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  std::string digestString = std::string(digest.begin(), digest.end());
  blob_handle.set_digest(std::move(digestString));

  BlobProto writeBlobProto = icing.OpenWriteBlob(blob_handle);
  ASSERT_THAT(writeBlobProto.status(), ProtoIsOk());
  {
    ScopedFd write_fd(writeBlobProto.file_descriptor());
    ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  }

  BlobProto commitBlobProto = icing.CommitBlob(blob_handle);
  ASSERT_THAT(commitBlobProto.status(), ProtoIsOk());

  // persist blob to disk
  EXPECT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

  // create second icing in 8 days later
  auto fake_clock2 = std::make_unique<FakeClock>();
  fake_clock2->SetSystemTimeMilliseconds(1000 + 8 * 24 * 60 * 60 *
                                                    1000);  // pass 8 days
  TestIcingSearchEngine icing2(GetDefaultIcingOptions(),
                               std::make_unique<Filesystem>(),
                               std::make_unique<IcingFilesystem>(),
                               std::move(fake_clock2), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  // Blob remain before optimize
  BlobProto readBlobProto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(readBlobProto.status(), ProtoIsOk());
  ScopedFd read_fd(readBlobProto.file_descriptor());

  uint64_t size = filesystem()->GetFileSize(*read_fd);
  std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
  filesystem()->Read(read_fd.get(), buf.get(), size);
  close(read_fd.get());

  std::string expected_data = std::string(data.begin(), data.end());
  std::string actual_data = std::string(buf.get(), buf.get() + size);
  EXPECT_EQ(expected_data, actual_data);

  // Optimize remove the expired orphan blob.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  EXPECT_THAT(icing2.OpenReadBlob(blob_handle).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineBlobTest, BlobOptimizeWithoutCommit) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // set a schema to icing to avoid wipe out all directories.
  ASSERT_THAT(icing.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());

  // write two blobs but not commit
  PropertyProto::BlobHandleProto blob_handle1;
  blob_handle1.set_label("label1");
  std::vector<unsigned char> data1 = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest1 = CalculateDigest(data1);
  std::string digestString1 = std::string(digest1.begin(), digest1.end());
  blob_handle1.set_digest(std::move(digestString1));
  BlobProto writeBlobProto = icing.OpenWriteBlob(blob_handle1);
  ASSERT_THAT(writeBlobProto.status(), ProtoIsOk());
  {
    ScopedFd write_fd(writeBlobProto.file_descriptor());
    ASSERT_TRUE(
        filesystem()->Write(write_fd.get(), data1.data(), data1.size()));
  }

  PropertyProto::BlobHandleProto blob_handle2;
  blob_handle2.set_label("label2");
  std::vector<unsigned char> data2 = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest2 = CalculateDigest(data2);
  std::string digestString2 = std::string(digest2.begin(), digest2.end());
  blob_handle2.set_digest(std::move(digestString2));
  writeBlobProto = icing.OpenWriteBlob(blob_handle2);
  ASSERT_THAT(writeBlobProto.status(), ProtoIsOk());
  {
    ScopedFd write_fd(writeBlobProto.file_descriptor());
    ASSERT_TRUE(
        filesystem()->Write(write_fd.get(), data2.data(), data2.size()));
  }
  // persist blob to disk
  EXPECT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

  // create second icing in 8 days later
  auto fake_clock2 = std::make_unique<FakeClock>();
  fake_clock2->SetSystemTimeMilliseconds(1000 + 8 * 24 * 60 * 60 *
                                                    1000);  // pass 8 days
  TestIcingSearchEngine icing2(GetDefaultIcingOptions(),
                               std::make_unique<Filesystem>(),
                               std::make_unique<IcingFilesystem>(),
                               std::move(fake_clock2), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  // Blob is able to commit before optimize
  EXPECT_THAT(icing2.CommitBlob(blob_handle1).status(), ProtoIsOk());
  // Optimize remove the expired orphan blob. so it's not able to commit.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  EXPECT_THAT(icing2.CommitBlob(blob_handle2).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineBlobTest, ReferenceCount) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("label");
  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  std::string digestString = std::string(digest.begin(), digest.end());
  blob_handle.set_digest(std::move(digestString));

  BlobProto writeBlobProto = icing.OpenWriteBlob(blob_handle);
  ASSERT_THAT(writeBlobProto.status(), ProtoIsOk());

  ScopedFd write_fd(writeBlobProto.file_descriptor());
  ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  close(write_fd.get());

  BlobProto commitBlobProto = icing.CommitBlob(blob_handle);
  ASSERT_THAT(commitBlobProto.status(), ProtoIsOk());

  // Set schema and put a document that contains the blob handle
  ASSERT_THAT(icing.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());
  ASSERT_THAT(
      icing.Put(CreateBlobDocument("namespace", "doc1", blob_handle)).status(),
      ProtoIsOk());

  // persist to disk
  EXPECT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

  // create second icing in 8 days later
  auto fake_clock2 = std::make_unique<FakeClock>();
  fake_clock2->SetSystemTimeMilliseconds(1000 + 8 * 24 * 60 * 60 *
                                                    1000);  // pass 8 days
  TestIcingSearchEngine icing2(GetDefaultIcingOptions(),
                               std::make_unique<Filesystem>(),
                               std::make_unique<IcingFilesystem>(),
                               std::move(fake_clock2), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  // Optimize won't remove the blob since there is reference document.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  BlobProto readBlobProto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(readBlobProto.status(), ProtoIsOk());
  {
    ScopedFd read_fd(readBlobProto.file_descriptor());
    uint64_t size = filesystem()->GetFileSize(*read_fd);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    ASSERT_TRUE(filesystem()->Read(read_fd.get(), buf.get(), size));

    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }

  // remove the reference document, now the blob is an orphan.
  ASSERT_THAT(icing2.Delete("namespace", "doc1").status(), ProtoIsOk());
  // The blob remain before optimize.
  readBlobProto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(readBlobProto.status(), ProtoIsOk());
  {
    ScopedFd read_fd2(readBlobProto.file_descriptor());

    uint64_t size = filesystem()->GetFileSize(*read_fd2);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    ASSERT_TRUE(filesystem()->Read(read_fd2.get(), buf.get(), size));

    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }

  // Optimize remove the expired orphan blob.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  EXPECT_THAT(icing2.OpenReadBlob(blob_handle).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineBlobTest, ReferenceCountNestedDocument) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("label");
  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  std::string digestString = std::string(digest.begin(), digest.end());
  blob_handle.set_digest(std::move(digestString));

  BlobProto writeBlobProto = icing.OpenWriteBlob(blob_handle);
  ASSERT_THAT(writeBlobProto.status(), ProtoIsOk());

  ScopedFd write_fd(writeBlobProto.file_descriptor());
  ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  close(write_fd.get());

  BlobProto commitBlobProto = icing.CommitBlob(blob_handle);
  ASSERT_THAT(commitBlobProto.status(), ProtoIsOk());

  // Set an multi-level schema and put a document that contains the blob handle
  // in the nested document property.
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("blob")
                           .SetDataType(TYPE_BLOB_HANDLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nestedDoc")
                  .SetDataTypeDocument("A", /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  ASSERT_THAT(
      icing.SetSchema(SchemaBuilder().AddType(type_a).AddType(type_b).Build())
          .status(),
      ProtoIsOk());
  DocumentProto document_a = DocumentBuilder()
                                 .SetKey("namespace", "doc_a")
                                 .SetSchema("A")
                                 .AddBlobHandleProperty("blob", blob_handle)
                                 .Build();
  DocumentProto document_b = DocumentBuilder()
                                 .SetKey("namespace", "doc_b")
                                 .SetSchema("B")
                                 .AddDocumentProperty("nestedDoc", document_a)
                                 .Build();
  ASSERT_THAT(icing.Put(document_b).status(), ProtoIsOk());

  // persist to disk
  EXPECT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

  // create second icing in 8 days later
  auto fake_clock2 = std::make_unique<FakeClock>();
  fake_clock2->SetSystemTimeMilliseconds(1000 + 8 * 24 * 60 * 60 *
                                                    1000);  // pass 8 days
  TestIcingSearchEngine icing2(GetDefaultIcingOptions(),
                               std::make_unique<Filesystem>(),
                               std::make_unique<IcingFilesystem>(),
                               std::move(fake_clock2), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  // Optimize won't remove the blob since there is reference document.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  BlobProto readBlobProto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(readBlobProto.status(), ProtoIsOk());
  {
    ScopedFd read_fd(readBlobProto.file_descriptor());
    uint64_t size = filesystem()->GetFileSize(*read_fd);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    ASSERT_TRUE(filesystem()->Read(read_fd.get(), buf.get(), size));

    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }

  // remove the reference document, now the blob is an orphan.
  ASSERT_THAT(icing2.Delete("namespace", "doc_b").status(), ProtoIsOk());
  // The blob remain before optimize.
  readBlobProto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(readBlobProto.status(), ProtoIsOk());
  {
    ScopedFd read_fd2(readBlobProto.file_descriptor());

    uint64_t size = filesystem()->GetFileSize(*read_fd2);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    ASSERT_TRUE(filesystem()->Read(read_fd2.get(), buf.get(), size));

    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }

  // Optimize remove the expired orphan blob.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  EXPECT_THAT(icing2.OpenReadBlob(blob_handle).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineBlobTest, OptimizeMultipleReferenceDocument) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("label");
  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  std::string digestString = std::string(digest.begin(), digest.end());
  blob_handle.set_digest(std::move(digestString));

  BlobProto writeBlobProto = icing.OpenWriteBlob(blob_handle);
  ASSERT_THAT(writeBlobProto.status(), ProtoIsOk());
  {
    ScopedFd write_fd(writeBlobProto.file_descriptor());
    ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  }

  BlobProto commitBlobProto = icing.CommitBlob(blob_handle);
  ASSERT_THAT(commitBlobProto.status(), ProtoIsOk());

  // Set schema and put 3 documents that contains the blob handle
  ASSERT_THAT(icing.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());
  ASSERT_THAT(
      icing.Put(CreateBlobDocument("namespace", "doc1", blob_handle)).status(),
      ProtoIsOk());
  ASSERT_THAT(
      icing.Put(CreateBlobDocument("namespace", "doc2", blob_handle)).status(),
      ProtoIsOk());
  ASSERT_THAT(
      icing.Put(CreateBlobDocument("namespace", "doc3", blob_handle)).status(),
      ProtoIsOk());

  // persist to disk
  EXPECT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

  // create second icing in 8 days later
  auto fake_clock2 = std::make_unique<FakeClock>();
  fake_clock2->SetSystemTimeMilliseconds(1000 + 8 * 24 * 60 * 60 *
                                                    1000);  // pass 8 days
  TestIcingSearchEngine icing2(GetDefaultIcingOptions(),
                               std::make_unique<Filesystem>(),
                               std::make_unique<IcingFilesystem>(),
                               std::move(fake_clock2), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  // Optimize won't remove the blob since there are reference documents.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  BlobProto readBlobProto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(readBlobProto.status(), ProtoIsOk());
  {
    ScopedFd read_fd(readBlobProto.file_descriptor());

    uint64_t size = filesystem()->GetFileSize(*read_fd);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    filesystem()->Read(read_fd.get(), buf.get(), size);
    close(read_fd.get());

    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }

  // remove two reference documents.
  ASSERT_THAT(icing2.Delete("namespace", "doc1").status(), ProtoIsOk());
  ASSERT_THAT(icing2.Delete("namespace", "doc2").status(), ProtoIsOk());
  // The blob remain after optimize.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  readBlobProto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(readBlobProto.status(), ProtoIsOk());
  {
    ScopedFd read_fd2(readBlobProto.file_descriptor());

    uint64_t size = filesystem()->GetFileSize(*read_fd2);
    std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
    filesystem()->Read(read_fd2.get(), buf.get(), size);
    close(read_fd2.get());

    std::string expected_data = std::string(data.begin(), data.end());
    std::string actual_data = std::string(buf.get(), buf.get() + size);
    EXPECT_EQ(expected_data, actual_data);
  }

  // remove the last reference document, now the blob become orphan.
  ASSERT_THAT(icing2.Delete("namespace", "doc3").status(), ProtoIsOk());
  // Optimize remove the expired orphan blob.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  EXPECT_THAT(icing2.OpenReadBlob(blob_handle).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineBlobTest, OptimizeMultipleBlobHandles) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle1;
  blob_handle1.set_label("label1");
  std::vector<unsigned char> data1 = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest1 = CalculateDigest(data1);
  std::string digestString1 = std::string(digest1.begin(), digest1.end());
  blob_handle1.set_digest(std::move(digestString1));

  BlobProto writeBlobProto1 = icing.OpenWriteBlob(blob_handle1);
  ASSERT_THAT(writeBlobProto1.status(), ProtoIsOk());
  {
    ScopedFd write_fd(writeBlobProto1.file_descriptor());
    ASSERT_TRUE(
        filesystem()->Write(write_fd.get(), data1.data(), data1.size()));
  }

  BlobProto commitBlobProto = icing.CommitBlob(blob_handle1);
  ASSERT_THAT(commitBlobProto.status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle2;
  blob_handle2.set_label("label2");
  std::vector<unsigned char> data2 = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest2 = CalculateDigest(data2);
  std::string digestString2 = std::string(digest2.begin(), digest2.end());
  blob_handle2.set_digest(std::move(digestString2));

  BlobProto writeBlobProto2 = icing.OpenWriteBlob(blob_handle2);
  ASSERT_THAT(writeBlobProto2.status(), ProtoIsOk());
  {
    ScopedFd write_fd(writeBlobProto2.file_descriptor());
    ASSERT_TRUE(
        filesystem()->Write(write_fd.get(), data2.data(), data2.size()));
  }

  BlobProto commitBlobProto2 = icing.CommitBlob(blob_handle2);
  ASSERT_THAT(commitBlobProto2.status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle3;
  blob_handle3.set_label("label3");
  std::vector<unsigned char> data3 = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest3 = CalculateDigest(data3);
  std::string digestString3 = std::string(digest3.begin(), digest3.end());
  blob_handle3.set_digest(std::move(digestString3));

  BlobProto writeBlobProto3 = icing.OpenWriteBlob(blob_handle3);
  ASSERT_THAT(writeBlobProto3.status(), ProtoIsOk());
  {
    ScopedFd write_fd(writeBlobProto3.file_descriptor());
    ASSERT_TRUE(
        filesystem()->Write(write_fd.get(), data3.data(), data3.size()));
  }

  BlobProto commitBlobProto3 = icing.CommitBlob(blob_handle3);
  ASSERT_THAT(commitBlobProto3.status(), ProtoIsOk());

  // Set schema and put 3 documents that contains the blob handle
  ASSERT_THAT(icing.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());
  ASSERT_THAT(
      icing.Put(CreateBlobDocument("namespace", "doc1", blob_handle1)).status(),
      ProtoIsOk());
  ASSERT_THAT(
      icing.Put(CreateBlobDocument("namespace", "doc2", blob_handle2)).status(),
      ProtoIsOk());
  ASSERT_THAT(
      icing.Put(CreateBlobDocument("namespace", "doc3", blob_handle3)).status(),
      ProtoIsOk());

  // persist to disk
  EXPECT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

  // create second icing in 8 days later
  auto fake_clock2 = std::make_unique<FakeClock>();
  fake_clock2->SetSystemTimeMilliseconds(1000 + 8 * 24 * 60 * 60 *
                                                    1000);  // pass 8 days
  TestIcingSearchEngine icing2(GetDefaultIcingOptions(),
                               std::make_unique<Filesystem>(),
                               std::make_unique<IcingFilesystem>(),
                               std::move(fake_clock2), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  // Optimize won't remove the blob since there are reference documents.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  ASSERT_THAT(icing2.OpenReadBlob(blob_handle1).status(), ProtoIsOk());
  ASSERT_THAT(icing2.OpenReadBlob(blob_handle2).status(), ProtoIsOk());
  ASSERT_THAT(icing2.OpenReadBlob(blob_handle3).status(), ProtoIsOk());

  // Remove first two reference documents.
  ASSERT_THAT(icing2.Delete("namespace", "doc1").status(), ProtoIsOk());
  ASSERT_THAT(icing2.Delete("namespace", "doc2").status(), ProtoIsOk());

  // First two orphan blobs are removed after optimize .
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  EXPECT_THAT(icing2.OpenReadBlob(blob_handle1).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
  EXPECT_THAT(icing2.OpenReadBlob(blob_handle2).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
  ASSERT_THAT(icing2.OpenReadBlob(blob_handle3).status(), ProtoIsOk());

  // remove the last reference document, now the all blobs become orphan.
  ASSERT_THAT(icing2.Delete("namespace", "doc3").status(), ProtoIsOk());
  // Optimize remove the expired orphan blob.
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  EXPECT_THAT(icing2.OpenReadBlob(blob_handle3).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineBlobTest, OptimizeBlobHandlesNoTTL) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(1000);
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_enable_blob_store(true);
  // set orphan blob ttl to 0, which means no ttl
  icing_options.set_orphan_blob_time_to_live_ms(0);
  TestIcingSearchEngine icing(icing_options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // set a schema to icing to avoid wipe out all directories.
  ASSERT_THAT(icing.SetSchema(CreateBlobSchema()).status(), ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_label("label");
  std::vector<unsigned char> data = GenerateRandomBytes(24);
  std::array<uint8_t, 32> digest = CalculateDigest(data);
  std::string digestString = std::string(digest.begin(), digest.end());
  blob_handle.set_digest(std::move(digestString));

  BlobProto writeBlobProto = icing.OpenWriteBlob(blob_handle);
  ASSERT_THAT(writeBlobProto.status(), ProtoIsOk());
  {
    ScopedFd write_fd(writeBlobProto.file_descriptor());
    ASSERT_TRUE(filesystem()->Write(write_fd.get(), data.data(), data.size()));
  }

  BlobProto commitBlobProto = icing.CommitBlob(blob_handle);
  ASSERT_THAT(commitBlobProto.status(), ProtoIsOk());

  // persist blob to disk
  EXPECT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

  // create second icing in a year later 365L * 24 * 60 * 60 * 1000;
  auto fake_clock2 = std::make_unique<FakeClock>();
  fake_clock2->SetSystemTimeMilliseconds(1471228928);
  TestIcingSearchEngine icing2(icing_options, std::make_unique<Filesystem>(),
                               std::make_unique<IcingFilesystem>(),
                               std::move(fake_clock2), GetTestJniCache());
  ASSERT_THAT(icing2.Initialize().status(), ProtoIsOk());

  // Blob remain after optimize
  ASSERT_THAT(icing2.Optimize().status(), ProtoIsOk());
  BlobProto readBlobProto = icing2.OpenReadBlob(blob_handle);
  ASSERT_THAT(readBlobProto.status(), ProtoIsOk());
  ScopedFd read_fd(readBlobProto.file_descriptor());

  uint64_t size = filesystem()->GetFileSize(*read_fd);
  std::unique_ptr<uint8_t[]> buf = std::make_unique<uint8_t[]>(size);
  filesystem()->Read(read_fd.get(), buf.get(), size);
  close(read_fd.get());

  std::string expected_data = std::string(data.begin(), data.end());
  std::string actual_data = std::string(buf.get(), buf.get() + size);
  EXPECT_EQ(expected_data, actual_data);
}

}  // namespace
}  // namespace lib
}  // namespace icing
