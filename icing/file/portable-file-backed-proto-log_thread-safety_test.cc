// Copyright (C) 2025 Google LLC
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

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {
namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::NotNull;

class PortableFileBackedProtoLogThreadSafetyTest
    : public ::testing::Test {
 protected:
  void SetUp() override {
    file_path_ = GetTestTempDir() + "/proto_log_thread_safety";
    filesystem_.DeleteFile(file_path_.c_str());
  }

  void TearDown() override { filesystem_.DeleteFile(file_path_.c_str()); }

  const Filesystem filesystem_;
  std::string file_path_;
};

TEST_F(PortableFileBackedProtoLogThreadSafetyTest, ConcurrentReadProto) {
  // Write some documents to the log.
  ICING_ASSERT_OK_AND_ASSIGN(
      auto create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              /*compress_in=*/true,
              /*max_proto_size_in=*/1024 * 1024,
              PortableFileBackedProtoLog<
                  DocumentProto>::kDefaultCompressionLevel,
              PortableFileBackedProtoLog<
                  DocumentProto>::kDefaultCompressionThresholdBytes,
              /*compression_mem_level_in=*/1)));
  std::unique_ptr<PortableFileBackedProtoLog<DocumentProto>> proto_log =
      std::move(create_result.proto_log);
  ASSERT_THAT(proto_log, NotNull());

  constexpr int kNumDocuments = 20;
  std::vector<DocumentProto> documents;
  std::vector<int64_t> offsets;
  for (int i = 0; i < kNumDocuments; ++i) {
    documents.push_back(
        DocumentBuilder()
            .SetKey("namespace", "uri" + std::to_string(i))
            .AddStringProperty("prop", std::string(100 * i, 'a'))
            .Build());
    ICING_ASSERT_OK_AND_ASSIGN(int64_t offset,
                               proto_log->WriteProto(documents.back()));
    offsets.push_back(offset);
  }
  ICING_ASSERT_OK(proto_log->PersistToDisk());

  // Create kNumThreads to call ReadProto concurrently.
  constexpr int kNumThreads = 50;
  constexpr int kNumReadsPerThread = 100;

  auto reader_task = [&]() {
    for (int i = 0; i < kNumReadsPerThread; ++i) {
      int doc_index = i % kNumDocuments;
      ICING_ASSERT_OK_AND_ASSIGN(DocumentProto read_doc,
                                 proto_log->ReadProto(offsets[doc_index]));
      EXPECT_THAT(read_doc, EqualsProto(documents[doc_index]));
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(reader_task);
  }

  for (auto& thread : threads) {
    thread.join();
  }
}



}  // namespace
}  // namespace lib
}  // namespace icing