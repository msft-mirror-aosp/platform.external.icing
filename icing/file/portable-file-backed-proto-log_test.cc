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

#include "icing/file/portable-file-backed-proto-log.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/mock-filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/gzip_stream.h"
#include "icing/proto/document.pb.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"
#include "icing/util/data-loss.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Eq;
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::Ne;
using ::testing::Not;
using ::testing::NotNull;

using Header = PortableFileBackedProtoLog<DocumentProto>::Header;

Header ReadHeader(Filesystem filesystem, const std::string& file_path) {
  Header header;
  filesystem.PRead(file_path.c_str(), &header, sizeof(Header),
                   /*offset=*/0);
  return header;
}

void WriteHeader(Filesystem filesystem, const std::string& file_path,
                 Header& header) {
  filesystem.Write(file_path.c_str(), &header, sizeof(Header));
}

libtextclassifier3::StatusOr<std::pair<int64_t, int64_t>>
WriteProtoAndReturnOffsetAndSize(
    PortableFileBackedProtoLog<DocumentProto>* proto_log,
    const DocumentProto& document) {
  ICING_ASSIGN_OR_RETURN(int64_t document_log_size_before,
                         proto_log->GetElementsFileSize());
  ICING_ASSIGN_OR_RETURN(int64_t offset, proto_log->WriteProto(document));
  ICING_ASSIGN_OR_RETURN(int64_t document_log_size_after,
                         proto_log->GetElementsFileSize());
  int64_t size_written = document_log_size_after - document_log_size_before;
  return std::make_pair(offset, size_written);
}

void CheckNewChecksums(const Header& header,
                       uint32_t expected_unsynced_tail_checksum,
                       uint32_t expected_header_checksum,
                       bool enable_new_header_format) {
  if (enable_new_header_format) {
    EXPECT_THAT(header.GetUnsyncedTailChecksum(),
                Eq(expected_unsynced_tail_checksum));
    EXPECT_THAT(header.GetHeaderChecksum(), Eq(expected_header_checksum));
  } else {
    EXPECT_THAT(header.GetUnsyncedTailChecksum(), Eq(0u));
    EXPECT_THAT(header.GetHeaderChecksum(), Eq(0u));
  }
}

class PortableFileBackedProtoLogTest : public ::testing::TestWithParam<bool> {
 protected:
  // Adds a user-defined default construct because a const member variable may
  // make the compiler accidentally delete the default constructor.
  // https://stackoverflow.com/a/47368753
  PortableFileBackedProtoLogTest() {}

  void SetUp() override {
    file_path_ = GetTestTempDir() + "/proto_log";
    filesystem_.DeleteFile(file_path_.c_str());
  }

  void TearDown() override { filesystem_.DeleteFile(file_path_.c_str()); }

  const Filesystem filesystem_;
  std::string file_path_;
  bool compress_ = true;
  int32_t compression_level_ =
      PortableFileBackedProtoLog<DocumentProto>::kDefaultCompressionLevel;
  uint32_t compression_threshold_bytes_ = PortableFileBackedProtoLog<
      DocumentProto>::kDefaultCompressionThresholdBytes;
  int32_t compression_mem_level_ = protobuf_ports::kDefaultMemLevel;
  int64_t max_proto_size_ = 256 * 1024;  // 256 KiB
  bool enable_smaller_decompression_buffer_size_ = true;
};

TEST_P(PortableFileBackedProtoLogTest, Initialize) {
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  EXPECT_THAT(create_result.proto_log, NotNull());
  EXPECT_FALSE(create_result.has_data_loss());
  EXPECT_FALSE(create_result.recalculated_checksum);

  // Can't recreate the same file with different options.
  ASSERT_THAT(PortableFileBackedProtoLog<DocumentProto>::Create(
                  &filesystem_, file_path_,
                  PortableFileBackedProtoLog<DocumentProto>::Options(
                      !compress_, max_proto_size_, compression_level_,
                      compression_threshold_bytes_, compression_mem_level_,
                      enable_smaller_decompression_buffer_size_,
                      /*enable_new_header_format_in=*/GetParam())),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_P(PortableFileBackedProtoLogTest, NewAndEmptyFileShouldFlushHeader) {
  // Mock the filesystem to verify that DataSync is called.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, DataSync(_)).Times(1);

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            mock_filesystem.get(), file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    EXPECT_THAT(create_result.proto_log, NotNull());
    EXPECT_FALSE(create_result.has_data_loss());
    EXPECT_FALSE(create_result.recalculated_checksum);
  }
}

TEST_P(PortableFileBackedProtoLogTest, InitializeValidatesOptions) {
  // max_proto_size must be greater than 0
  int invalid_max_proto_size = 0;
  ASSERT_THAT(PortableFileBackedProtoLog<DocumentProto>::Create(
                  &filesystem_, file_path_,
                  PortableFileBackedProtoLog<DocumentProto>::Options(
                      compress_, invalid_max_proto_size, compression_level_,
                      compression_threshold_bytes_, compression_mem_level_,
                      enable_smaller_decompression_buffer_size_,
                      /*enable_new_header_format_in=*/GetParam())),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // max_proto_size must be under 16 MiB
  invalid_max_proto_size = 16 * 1024 * 1024;
  ASSERT_THAT(PortableFileBackedProtoLog<DocumentProto>::Create(
                  &filesystem_, file_path_,
                  PortableFileBackedProtoLog<DocumentProto>::Options(
                      compress_, invalid_max_proto_size, compression_level_,
                      compression_threshold_bytes_, compression_mem_level_,
                      enable_smaller_decompression_buffer_size_,
                      /*enable_new_header_format_in=*/GetParam())),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // compression_level must be between 0 and 9 inclusive
  int invalid_compression_level = -1;
  ASSERT_THAT(PortableFileBackedProtoLog<DocumentProto>::Create(
                  &filesystem_, file_path_,
                  PortableFileBackedProtoLog<DocumentProto>::Options(
                      compress_, max_proto_size_, invalid_compression_level,
                      compression_threshold_bytes_, compression_mem_level_,
                      enable_smaller_decompression_buffer_size_,
                      /*enable_new_header_format_in=*/GetParam())),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // compression_level must be between 0 and 9 inclusive
  invalid_compression_level = 10;
  ASSERT_THAT(PortableFileBackedProtoLog<DocumentProto>::Create(
                  &filesystem_, file_path_,
                  PortableFileBackedProtoLog<DocumentProto>::Options(
                      compress_, max_proto_size_, invalid_compression_level,
                      compression_threshold_bytes_, compression_mem_level_,
                      enable_smaller_decompression_buffer_size_,
                      /*enable_new_header_format_in=*/GetParam())),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // compression_mem_level must be between 1 and 9 inclusive
  int invalid_compression_mem_level = 0;
  ASSERT_THAT(
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, invalid_compression_mem_level,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // compression_mem_level must be between 1 and 9 inclusive
  invalid_compression_mem_level = 10;
  ASSERT_THAT(
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, invalid_compression_mem_level,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_P(PortableFileBackedProtoLogTest, ReservedSpaceForHeader) {
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));

  // With no protos written yet, the log should be minimum the size of the
  // reserved header space.
  ASSERT_EQ(filesystem_.GetFileSize(file_path_.c_str()),
            PortableFileBackedProtoLog<DocumentProto>::kHeaderReservedBytes);
}

TEST_P(PortableFileBackedProtoLogTest, WriteProtoTooLarge) {
  int max_proto_size = 1;
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  DocumentProto document = DocumentBuilder().SetKey("namespace", "uri").Build();

  // Proto is too large for the max_proto_size_in
  ASSERT_THAT(proto_log->WriteProto(document),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_P(PortableFileBackedProtoLogTest, ReadProtoWrongKProtoMagic) {
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  // Write a proto
  DocumentProto document = DocumentBuilder().SetKey("namespace", "uri").Build();

  ICING_ASSERT_OK_AND_ASSIGN(int64_t file_offset,
                             proto_log->WriteProto(document));

  // The 4 bytes of metadata that just doesn't have the same kProtoMagic
  // specified in file-backed-proto-log.h
  uint32_t wrong_magic = 0x7E000000;

  // Sanity check that we opened the file correctly
  int fd = filesystem_.OpenForWrite(file_path_.c_str());
  ASSERT_GT(fd, 0);

  // Write the wrong kProtoMagic in, kProtoMagics are stored at the beginning of
  // a proto entry.
  filesystem_.PWrite(fd, file_offset, &wrong_magic, sizeof(wrong_magic));

  ASSERT_THAT(proto_log->ReadProto(file_offset),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_P(PortableFileBackedProtoLogTest, ReadWriteUncompressedProto) {
  int last_offset;
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/false, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write the first proto
    DocumentProto document1 =
        DocumentBuilder().SetKey("namespace1", "uri1").Build();

    ICING_ASSERT_OK_AND_ASSIGN(int written_position,
                               proto_log->WriteProto(document1));

    int document1_offset = written_position;

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(written_position),
                IsOkAndHolds(EqualsProto(document1)));

    // Write a second proto that's close to the max size. Leave some room for
    // the rest of the proto properties.
    std::string long_str(max_proto_size_ - 1024, 'a');
    DocumentProto document2 = DocumentBuilder()
                                  .SetKey("namespace2", "uri2")
                                  .AddStringProperty("long_str", long_str)
                                  .Build();

    ICING_ASSERT_OK_AND_ASSIGN(written_position,
                               proto_log->WriteProto(document2));

    int document2_offset = written_position;
    last_offset = written_position;
    ASSERT_GT(document2_offset, document1_offset);

    // Check the second proto
    ASSERT_THAT(proto_log->ReadProto(written_position),
                IsOkAndHolds(EqualsProto(document2)));

    ICING_ASSERT_OK(proto_log->PersistToDisk());
  }

  {
    // Make a new proto_log with the same file_path, and make sure we
    // can still write to the same underlying file.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/false, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write a third proto
    DocumentProto document3 =
        DocumentBuilder().SetKey("namespace3", "uri3").Build();

    ASSERT_THAT(recreated_proto_log->WriteProto(document3),
                IsOkAndHolds(Gt(last_offset)));
  }
}

TEST_P(PortableFileBackedProtoLogTest, ReadWriteCompressedProto) {
  int last_offset;

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write the first proto
    DocumentProto document1 =
        DocumentBuilder().SetKey("namespace1", "uri1").Build();

    ICING_ASSERT_OK_AND_ASSIGN(int written_position,
                               proto_log->WriteProto(document1));

    int document1_offset = written_position;

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(written_position),
                IsOkAndHolds(EqualsProto(document1)));

    // Write a second proto that's close to the max size. Leave some room for
    // the rest of the proto properties.
    std::string long_str(max_proto_size_ - 1024, 'a');
    DocumentProto document2 = DocumentBuilder()
                                  .SetKey("namespace2", "uri2")
                                  .AddStringProperty("long_str", long_str)
                                  .Build();

    ICING_ASSERT_OK_AND_ASSIGN(written_position,
                               proto_log->WriteProto(document2));

    int document2_offset = written_position;
    last_offset = written_position;
    ASSERT_GT(document2_offset, document1_offset);

    // Check the second proto
    ASSERT_THAT(proto_log->ReadProto(written_position),
                IsOkAndHolds(EqualsProto(document2)));

    ICING_ASSERT_OK(proto_log->PersistToDisk());
  }

  {
    // Make a new proto_log with the same file_path, and make sure we
    // can still write to the same underlying file.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write a third proto
    DocumentProto document3 =
        DocumentBuilder().SetKey("namespace3", "uri3").Build();

    ASSERT_THAT(recreated_proto_log->WriteProto(document3),
                IsOkAndHolds(Gt(last_offset)));
  }
}

TEST_P(PortableFileBackedProtoLogTest, ReadWriteDifferentCompressionLevel) {
  int document1_offset;
  int document2_offset;
  int document3_offset;

  // The first proto to write that's close to the max size. Leave some room for
  // the rest of the proto properties.
  std::string long_str(max_proto_size_ - 1024, 'a');
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace1", "uri1")
                                .AddStringProperty("long_str", long_str)
                                .Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace2", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace3", "uri3").Build();

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/3,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write the first proto
    ICING_ASSERT_OK_AND_ASSIGN(document1_offset,
                               proto_log->WriteProto(document1));

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    ICING_ASSERT_OK(proto_log->PersistToDisk());
  }

  // Make a new proto_log with the same file_path but different compression
  // level, and make sure we can still read from and write to the same
  // underlying file.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/9,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Check the first proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Write a second proto
    ICING_ASSERT_OK_AND_ASSIGN(document2_offset,
                               recreated_proto_log->WriteProto(document2));

    ASSERT_GT(document2_offset, document1_offset);

    // Check the second proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    ICING_ASSERT_OK(recreated_proto_log->PersistToDisk());
  }

  // One more time but with 0 compression level
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/0,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Check the first proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Check the second proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    // Write a third proto
    ICING_ASSERT_OK_AND_ASSIGN(document3_offset,
                               recreated_proto_log->WriteProto(document3));

    ASSERT_GT(document3_offset, document2_offset);

    // Check the third proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document3_offset),
                IsOkAndHolds(EqualsProto(document3)));
  }
}

TEST_P(PortableFileBackedProtoLogTest, ReadWriteDifferentCompressionMemLevel) {
  int document1_offset;
  int document2_offset;
  int document3_offset;

  // The first proto to write that's close to the max size. Leave some room for
  // the rest of the proto properties.
  std::string long_str(max_proto_size_ - 1024, 'a');
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace1", "uri1")
                                .AddStringProperty("long_str", long_str)
                                .Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace2", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace3", "uri3").Build();

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_, compression_level_,
                compression_threshold_bytes_,
                /*compression_mem_level_in=*/8,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write the first proto
    ICING_ASSERT_OK_AND_ASSIGN(document1_offset,
                               proto_log->WriteProto(document1));

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    ICING_ASSERT_OK(proto_log->PersistToDisk());
  }

  // Make a new proto_log with the same file_path but different compression mem
  // level, and make sure we can still read from and write to the same
  // underlying file.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_, compression_level_,
                compression_threshold_bytes_,
                /*compression_mem_level_in=*/1,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Check the first proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Write a second proto
    ICING_ASSERT_OK_AND_ASSIGN(document2_offset,
                               recreated_proto_log->WriteProto(document2));

    ASSERT_GT(document2_offset, document1_offset);

    // Check the second proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    ICING_ASSERT_OK(recreated_proto_log->PersistToDisk());
  }

  // One more time but with 9 compression mem level
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_, compression_level_,
                compression_threshold_bytes_,
                /*compression_mem_level_in=*/9,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Check the first proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Check the second proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    // Write a third proto
    ICING_ASSERT_OK_AND_ASSIGN(document3_offset,
                               recreated_proto_log->WriteProto(document3));

    ASSERT_GT(document3_offset, document2_offset);

    // Check the third proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document3_offset),
                IsOkAndHolds(EqualsProto(document3)));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       ReadWriteEnableAndDisableSmallerDecompressionBufferSize) {
  int document1_offset;
  int document2_offset;
  int document3_offset;

  // The first proto to write that's close to the max size. Leave some room for
  // the rest of the proto properties.
  std::string long_str(max_proto_size_ - 1024, 'a');
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace1", "uri1")
                                .AddStringProperty("long_str", long_str)
                                .Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace2", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace3", "uri3").Build();

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                /*enable_smaller_decompression_buffer_size_in=*/false,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write the first proto
    ICING_ASSERT_OK_AND_ASSIGN(document1_offset,
                               proto_log->WriteProto(document1));

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    ICING_ASSERT_OK(proto_log->PersistToDisk());
  }

  // Make a new proto_log with the same file_path with smaller decompression
  // buffer size enabled, and make sure we can still read from and write to the
  // same underlying file.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                /*enable_smaller_decompression_buffer_size_in=*/true,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Check the first proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Write a second proto
    ICING_ASSERT_OK_AND_ASSIGN(document2_offset,
                               recreated_proto_log->WriteProto(document2));

    ASSERT_GT(document2_offset, document1_offset);

    // Check the second proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    ICING_ASSERT_OK(recreated_proto_log->PersistToDisk());
  }

  // One more time but with smaller decompression buffer size disabled
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                /*enable_smaller_decompression_buffer_size_in=*/false,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Check the first proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Check the second proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    // Write a third proto
    ICING_ASSERT_OK_AND_ASSIGN(document3_offset,
                               recreated_proto_log->WriteProto(document3));

    ASSERT_GT(document3_offset, document2_offset);

    // Check the third proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document3_offset),
                IsOkAndHolds(EqualsProto(document3)));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       WriteDifferentCompressionLevelDifferentSizes) {
  int document_log_size_with_compression_3;
  int document_log_size_with_no_compression;

  // The first proto to write that's close to the max size. Leave some room for
  // the rest of the proto properties.
  std::string long_str(max_proto_size_ - 1024, 'a');
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace1", "uri1")
                                .AddStringProperty("long_str", long_str)
                                .Build();

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/3, compression_threshold_bytes_,
                compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write the proto
    ICING_ASSERT_OK(proto_log->WriteProto(document1));
    ICING_ASSERT_OK(proto_log->PersistToDisk());

    document_log_size_with_compression_3 =
        filesystem_.GetFileSize(file_path_.c_str());
  }

  // Delete the proto_log so we can reuse the file_path
  filesystem_.DeleteFile(file_path_.c_str());

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/0, compression_threshold_bytes_,
                compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write the proto
    ICING_ASSERT_OK(proto_log->WriteProto(document1));
    ICING_ASSERT_OK(proto_log->PersistToDisk());

    document_log_size_with_no_compression =
        filesystem_.GetFileSize(file_path_.c_str());

    // Uncompressed document file size should be larger than original compressed
    // document file size
    ASSERT_GT(document_log_size_with_no_compression,
              document_log_size_with_compression_3);
  }
}

TEST_P(PortableFileBackedProtoLogTest, CompressionThreshold) {
  // Create document1 with size of 1024 bytes.
  int64_t document1_size = 1024;
  int64_t document1_offset;
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .AddStringProperty("long_str", std::string(990, 'a'))
          .Build();
  ASSERT_THAT(document1.ByteSizeLong(), Eq(document1_size));

  // Create document2 with size of 900 bytes.
  int64_t document2_size = 900;
  int64_t document2_offset;
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .AddStringProperty("long_str", std::string(866, 'a'))
          .Build();
  ASSERT_THAT(document2.ByteSizeLong(), Eq(document2_size));

  {
    // Create a proto_log with compression threshold of 1000 bytes
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/3,
                /*compression_threshold_bytes_in=*/1000, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    ICING_ASSERT_OK_AND_ASSIGN(
        auto document1_write_result,
        WriteProtoAndReturnOffsetAndSize(proto_log.get(), document1));
    document1_offset = document1_write_result.first;
    int64_t document1_size_written = document1_write_result.second;
    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));
    // The document should be compressed since it's size is larger than the
    // compression threshold.
    ASSERT_LT(document1_size_written, document1_size);

    ICING_ASSERT_OK_AND_ASSIGN(
        auto document2_write_result,
        WriteProtoAndReturnOffsetAndSize(proto_log.get(), document2));
    document2_offset = document2_write_result.first;
    int64_t document2_size_written = document2_write_result.second;
    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));
    // The document should be uncompressed since it's size is smaller than the
    // compression threshold.
    ASSERT_GE(document2_size_written, document2_size);

    ICING_ASSERT_OK(proto_log->PersistToDisk());
  }

  {
    // Make a new proto_log with the same file_path, and make sure we can still
    // read and write to the same underlying file.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/3,
                /*compression_threshold_bytes_in=*/1000, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Check the first proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Check the second proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    // Write a third proto
    DocumentProto document3 =
        DocumentBuilder().SetKey("namespace3", "uri3").Build();
    ICING_ASSERT_OK_AND_ASSIGN(int64_t document3_offset,
                               recreated_proto_log->WriteProto(document3));
    ASSERT_GT(document3_offset, document2_offset);
    // Check the third proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document3_offset),
                IsOkAndHolds(EqualsProto(document3)));
  }
}

TEST_P(PortableFileBackedProtoLogTest, ChangingCompressionThresholdIsOk) {
  // Create document1 with size of 1024 bytes.
  int64_t document1_size = 1024;
  int64_t document1_offset;
  int64_t document1_size_written;
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .AddStringProperty("long_str", std::string(990, 'a'))
          .Build();
  ASSERT_THAT(document1.ByteSizeLong(), Eq(document1_size));

  // Create document2 with size of 900 bytes.
  int64_t document2_size = 900;
  int64_t document2_offset;
  int64_t document2_size_written;
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .AddStringProperty("long_str", std::string(866, 'a'))
          .Build();
  ASSERT_THAT(document2.ByteSizeLong(), Eq(document2_size));

  {
    // Create a proto_log with compression threshold of 1000 bytes
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/3,
                /*compression_threshold_bytes_in=*/1000, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    ICING_ASSERT_OK_AND_ASSIGN(
        auto document1_write_result,
        WriteProtoAndReturnOffsetAndSize(proto_log.get(), document1));
    document1_offset = document1_write_result.first;
    document1_size_written = document1_write_result.second;
    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));
    // The document should be compressed since it's size is larger than the
    // compression threshold.
    ASSERT_LT(document1_size_written, document1_size);

    ICING_ASSERT_OK_AND_ASSIGN(
        auto document2_write_result,
        WriteProtoAndReturnOffsetAndSize(proto_log.get(), document2));
    document2_offset = document2_write_result.first;
    document2_size_written = document2_write_result.second;
    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));
    // The document should be uncompressed since it's size is smaller than the
    // compression threshold.
    ASSERT_GE(document2_size_written, document2_size);

    ICING_ASSERT_OK(proto_log->PersistToDisk());
  }

  {
    // Make a new proto_log with the same file_path with a different compression
    // threshold, and make sure we can still read and write to the same
    // underlying file.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                /*compress_in=*/true, max_proto_size_,
                /*compression_level_in=*/3,
                /*compression_threshold_bytes_in=*/100, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Check the first proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Check the second proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    // Write document1 again as the third proto;
    ICING_ASSERT_OK_AND_ASSIGN(
        auto document3_write_result,
        WriteProtoAndReturnOffsetAndSize(recreated_proto_log.get(), document1));
    int64_t document3_offset = document3_write_result.first;
    int64_t document3_size_written = document3_write_result.second;
    // With this new compression threshold, document1 should still be
    // compressed.
    ASSERT_EQ(document3_size_written, document1_size_written);
    ASSERT_GT(document3_offset, document2_offset);
    // Check the third proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document3_offset),
                IsOkAndHolds(EqualsProto(document1)));

    // Write document2 again as the fourth proto;
    ICING_ASSERT_OK_AND_ASSIGN(
        auto document4_write_result,
        WriteProtoAndReturnOffsetAndSize(recreated_proto_log.get(), document2));
    int64_t document4_offset = document4_write_result.first;
    int64_t document4_size_written = document4_write_result.second;
    // With this new compression threshold, document2 should now be compressed.
    ASSERT_LT(document4_size_written, document2_size_written);
    ASSERT_LT(document4_size_written, document2_size);
    ASSERT_GT(document4_offset, document3_offset);
    // Check the fourth proto
    ASSERT_THAT(recreated_proto_log->ReadProto(document4_offset),
                IsOkAndHolds(EqualsProto(document2)));
  }
}

TEST_P(PortableFileBackedProtoLogTest, CorruptedLegacyHeaderSection) {
  // This test simulates the following scenario:
  // - Log checksum matches.
  // - Unsynced tail checksum matches.
  // - Legacy header checksum doesn't match.
  // - Header checksum matches.
  //
  // Expected behavior: full data loss.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    EXPECT_FALSE(create_result.has_data_loss());
  }

  int corrupt_legacy_header_checksum = 24;

  // Manually overwrite the legacy header checksum to simulate legacy header
  // section is corrupted.
  Header corrupted_header = ReadHeader(filesystem_, file_path_);
  corrupted_header.SetLegacyHeaderChecksum(corrupt_legacy_header_checksum);
  if (GetParam()) {
    corrupted_header.SetHeaderChecksum(
        corrupted_header.CalculateHeaderChecksum().Get());
  }
  WriteHeader(filesystem_, file_path_, corrupted_header);

  {
    // Reinitialize the same proto_log
    ASSERT_THAT(PortableFileBackedProtoLog<DocumentProto>::Create(
                    &filesystem_, file_path_,
                    PortableFileBackedProtoLog<DocumentProto>::Options(
                        compress_, max_proto_size_, compression_level_,
                        compression_threshold_bytes_, compression_mem_level_,
                        enable_smaller_decompression_buffer_size_,
                        /*enable_new_header_format_in=*/GetParam())),
                StatusIs(libtextclassifier3::StatusCode::INTERNAL,
                         HasSubstr("Invalid legacy header checksum")));
  }
}

TEST_P(PortableFileBackedProtoLogTest, CorruptedHeaderChecksum) {
  if (!GetParam()) {
    GTEST_SKIP() << "This test is only relevant to the new header format.";
  }

  // This test simulates the following scenario:
  // - Log checksum matches.
  // - Unsynced tail checksum matches.
  // - Legacy header checksum matches.
  // - Header checksum doesn't match.
  //
  // Expected behavior:
  // - Due to compatibility issue, PortableFileBackedProtoLog should assume that
  //   the existing dataset is generated by an older version which does not have
  //   the new header checksum logic.
  // - Should only throw away the data in the unsynced tail section.
  // - Related checksums should be recomputed and rewritten during
  //   initialization.

  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();
  DocumentProto document4 =
      DocumentBuilder().SetKey("namespace", "uri4").Build();
  DocumentProto document5 =
      DocumentBuilder().SetKey("namespace", "uri5").Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  EXPECT_FALSE(create_result.has_data_loss());

  // Write 3 protos to the log and call PersistToDisk to make them into log
  // checksummed section. Record the header at this state.
  ICING_ASSERT_OK(proto_log->WriteProto(document1));
  ICING_ASSERT_OK(proto_log->WriteProto(document2));
  ICING_ASSERT_OK(proto_log->WriteProto(document3));
  ICING_ASSERT_OK(proto_log->PersistToDisk());
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Crc32(3230460271u)));

  // Read the header for the state of doc1-to-doc3.
  Header header_doc1_to_doc3 = ReadHeader(filesystem_, file_path_);
  // Sanity check that the unsynced tail checksum is 0.
  ASSERT_THAT(header_doc1_to_doc3.GetUnsyncedTailChecksum(), Eq(0));

  // Write 2 more protos and keep them in the unsynced tail section (don't call
  // PersistToDisk, UpdateChecksum, or destructor).
  ICING_ASSERT_OK(proto_log->WriteProto(document4));
  ICING_ASSERT_OK(proto_log->WriteProto(document5));
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Crc32(705827520u)));

  // Read the header for the state of doc1-to-doc5.
  Header header_doc1_to_doc5 = ReadHeader(filesystem_, file_path_);
  // Sanity check:
  // - The legacy and log checksums are same as doc1-to-doc3.
  // - The unsynced tail checksum and header checksum are different from state
  //   doc1-to-doc3.
  ASSERT_THAT(header_doc1_to_doc5.GetLegacyHeaderChecksum(),
              Eq(header_doc1_to_doc3.GetLegacyHeaderChecksum()));
  ASSERT_THAT(header_doc1_to_doc5.GetLogChecksum(),
              Eq(header_doc1_to_doc3.GetLogChecksum()));
  ASSERT_THAT(header_doc1_to_doc5.GetUnsyncedTailChecksum(),
              Ne(header_doc1_to_doc3.GetUnsyncedTailChecksum()));
  ASSERT_THAT(header_doc1_to_doc5.GetHeaderChecksum(),
              Ne(header_doc1_to_doc3.GetHeaderChecksum()));

  // Manually overwrite the header checksum.
  Header corrupted_header = header_doc1_to_doc5;
  corrupted_header.SetHeaderChecksum(corrupted_header.GetHeaderChecksum() +
                                     12345);
  WriteHeader(filesystem_, file_path_, corrupted_header);

  // Initialize another instance with the same proto file. This should only
  // trigger partial data loss. IOW, we only throw away the data in the unsynced
  // tail section, so the data state is reverted back to doc1-to-doc3.
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result2,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  EXPECT_THAT(create_result2.data_loss, Eq(DataLoss::PARTIAL));

  // Check the overall checksum.
  EXPECT_THAT(create_result2.proto_log->GetChecksum(),
              IsOkAndHolds(Crc32(3230460271u)));

  // Manually check the header.
  Header header_after = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_after.GetUnsyncedTailChecksum(),
              Eq(header_doc1_to_doc3.GetUnsyncedTailChecksum()));
  EXPECT_THAT(header_after.GetHeaderChecksum(),
              Eq(header_doc1_to_doc3.GetHeaderChecksum()));
}

TEST_P(PortableFileBackedProtoLogTest, CorruptedUnsyncedTailChecksum) {
  if (!GetParam()) {
    GTEST_SKIP() << "This test is only relevant to the new header format.";
  }

  // This test simulates the following scenario:
  // - Log checksum matches.
  // - Unsynced tail checksum doesn't match.
  // - Legacy header checksum matches.
  // - Header checksum matches.
  //
  // Expected behavior:
  // - Should only throw away the data in the unsynced tail section.
  // - Related checksums should be recomputed and rewritten during
  //   initialization.

  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();
  DocumentProto document4 =
      DocumentBuilder().SetKey("namespace", "uri4").Build();
  DocumentProto document5 =
      DocumentBuilder().SetKey("namespace", "uri5").Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  EXPECT_FALSE(create_result.has_data_loss());

  // Write 3 protos to the log and call PersistToDisk to make them into log
  // checksummed section. Record the header at this state.
  ICING_ASSERT_OK(proto_log->WriteProto(document1));
  ICING_ASSERT_OK(proto_log->WriteProto(document2));
  ICING_ASSERT_OK(proto_log->WriteProto(document3));
  ICING_ASSERT_OK(proto_log->PersistToDisk());
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Crc32(3230460271u)));

  // Read the header for the state of doc1-to-doc3.
  Header header_doc1_to_doc3 = ReadHeader(filesystem_, file_path_);
  // Sanity check that the unsynced tail checksum is 0.
  ASSERT_THAT(header_doc1_to_doc3.GetUnsyncedTailChecksum(), Eq(0));

  // Write 2 more protos and keep them in the unsynced tail section (don't call
  // PersistToDisk, UpdateChecksum, or destructor).
  ICING_ASSERT_OK(proto_log->WriteProto(document4));
  ICING_ASSERT_OK(proto_log->WriteProto(document5));
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Crc32(705827520u)));

  // Read the header for the state of doc1-to-doc5.
  Header header_doc1_to_doc5 = ReadHeader(filesystem_, file_path_);
  // Sanity check that the unsynced tail checksum and header checksum are
  // different from state doc1-to-doc3.
  ASSERT_THAT(header_doc1_to_doc5.GetUnsyncedTailChecksum(),
              Ne(header_doc1_to_doc3.GetUnsyncedTailChecksum()));
  ASSERT_THAT(header_doc1_to_doc5.GetHeaderChecksum(),
              Ne(header_doc1_to_doc3.GetHeaderChecksum()));

  // Manually overwrite the unsynced tail checksum and recompute header
  // checksum.
  Header corrupted_header = header_doc1_to_doc5;
  corrupted_header.SetUnsyncedTailChecksum(
      corrupted_header.GetUnsyncedTailChecksum() + 12345);
  corrupted_header.SetHeaderChecksum(
      corrupted_header.CalculateHeaderChecksum().Get());
  WriteHeader(filesystem_, file_path_, corrupted_header);

  // Initialize another instance with the same proto file. This should only
  // trigger partial data loss. IOW, we only throw away the data in the unsynced
  // tail section, so the data state is reverted back to doc1-to-doc3.
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result2,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  EXPECT_THAT(create_result2.data_loss, Eq(DataLoss::PARTIAL));

  // Check the overall checksum.
  EXPECT_THAT(create_result2.proto_log->GetChecksum(),
              IsOkAndHolds(Crc32(3230460271u)));

  // Manually check the header.
  Header header_after = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_after.GetUnsyncedTailChecksum(),
              Eq(header_doc1_to_doc3.GetUnsyncedTailChecksum()));
  EXPECT_THAT(header_after.GetHeaderChecksum(),
              Eq(header_doc1_to_doc3.GetHeaderChecksum()));
}

TEST_P(PortableFileBackedProtoLogTest, DifferentMagic) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto recreated_proto_log = std::move(create_result.proto_log);
    EXPECT_FALSE(create_result.has_data_loss());

    // Corrupt the magic that's stored at the beginning of the header.
    int invalid_magic = -1;
    ASSERT_THAT(invalid_magic, Not(Eq(Header::kMagic)));

    // Write the corrupted header
    Header header = ReadHeader(filesystem_, file_path_);
    header.SetMagic(invalid_magic);
    WriteHeader(filesystem_, file_path_, header);
  }

  {
    // Reinitialize the same proto_log
    ASSERT_THAT(
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())),
        StatusIs(
            libtextclassifier3::StatusCode::INTERNAL,
            HasSubstr("Invalid header magic for PortableFileBackedProtoLog")));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       UnableToDetectCorruptContentWithoutDirtyBit) {
  // This is intentional that we can't detect corruption. We're trading off
  // earlier corruption detection for lower initialization latency. By not
  // calculating the checksum on initialization, we can initialize much faster,
  // but at the cost of detecting corruption. Note that even if we did detect
  // corruption, there was nothing we could've done except throw an error to
  // clients. We'll still do that, but at some later point when the log is
  // attempting to be accessed and we can't actually deserialize a proto from
  // it. See the description in cl/374278280 for more details.

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    EXPECT_FALSE(create_result.has_data_loss());

    DocumentProto document =
        DocumentBuilder().SetKey("namespace1", "uri1").Build();

    // Write and persist an document.
    ICING_ASSERT_OK_AND_ASSIGN(int64_t document_offset,
                               proto_log->WriteProto(document));
    ICING_ASSERT_OK(proto_log->PersistToDisk());

    // "Corrupt" the content written in the log.
    document.set_uri("invalid");
    std::string serialized_document = document.SerializeAsString();
    ASSERT_TRUE(filesystem_.PWrite(file_path_.c_str(), document_offset,
                                   serialized_document.data(),
                                   serialized_document.size()));
  }

  {
    // We can recover, and we don't have data loss.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    EXPECT_FALSE(create_result.has_data_loss());
    EXPECT_THAT(create_result.data_loss, Eq(DataLoss::NONE));
    EXPECT_FALSE(create_result.recalculated_checksum);

    // We still have the corrupted content in our file, we didn't throw
    // everything out.
    EXPECT_THAT(
        filesystem_.GetFileSize(file_path_.c_str()),
        Gt(PortableFileBackedProtoLog<DocumentProto>::kHeaderReservedBytes));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       DetectAndThrowOutCorruptContentWithDirtyBit) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    DocumentProto document =
        DocumentBuilder()
            .SetKey("namespace1", "uri1")
            .AddStringProperty("string_property", "foo", "bar")
            .Build();

    // Write and persist the protos
    ICING_ASSERT_OK_AND_ASSIGN(int64_t document_offset,
                               proto_log->WriteProto(document));

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document_offset),
                IsOkAndHolds(EqualsProto(document)));
  }

  {
    // "Corrupt" the content written in the log. Make the corrupt document
    // smaller than our original one so we don't accidentally write past our
    // file.
    DocumentProto document =
        DocumentBuilder().SetKey("invalid_namespace", "invalid_uri").Build();
    std::string serialized_document = document.SerializeAsString();
    ASSERT_TRUE(filesystem_.PWrite(
        file_path_.c_str(),
        PortableFileBackedProtoLog<DocumentProto>::kHeaderReservedBytes,
        serialized_document.data(), serialized_document.size()));

    Header header = ReadHeader(filesystem_, file_path_);

    // Set dirty bit to true to reflect that something changed in the log.
    header.SetDirtyFlag(true);
    header.UpdateHeaderChecksums(/*enable_new_header_format=*/GetParam());

    WriteHeader(filesystem_, file_path_, header);
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    EXPECT_TRUE(create_result.has_data_loss());
    EXPECT_THAT(create_result.data_loss, Eq(DataLoss::COMPLETE));

    // We had to recalculate the checksum to detect the corruption.
    EXPECT_TRUE(create_result.recalculated_checksum);

    // We lost everything, file size is back down to the header.
    EXPECT_THAT(
        filesystem_.GetFileSize(file_path_.c_str()),
        Eq(PortableFileBackedProtoLog<DocumentProto>::kHeaderReservedBytes));

    // At least the log is no longer dirty.
    Header header = ReadHeader(filesystem_, file_path_);
    EXPECT_FALSE(header.GetDirtyFlag());
  }
}

TEST_P(PortableFileBackedProtoLogTest, DirtyBitFalseAlarmKeepsData) {
  DocumentProto document =
      DocumentBuilder().SetKey("namespace1", "uri1").Build();
  int64_t document_offset;
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write and persist the first proto
    ICING_ASSERT_OK_AND_ASSIGN(document_offset,
                               proto_log->WriteProto(document));

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document_offset),
                IsOkAndHolds(EqualsProto(document)));
  }

  {
    Header header = ReadHeader(filesystem_, file_path_);

    // Simulate the dirty flag set as true, but no data has been changed yet.
    // Maybe we crashed between writing the dirty flag and erasing a proto.
    header.SetDirtyFlag(true);
    header.UpdateHeaderChecksums(/*enable_new_header_format=*/GetParam());

    WriteHeader(filesystem_, file_path_, header);
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    EXPECT_FALSE(create_result.has_data_loss());

    // Even though nothing changed, the false alarm dirty bit should have
    // triggered us to recalculate our checksum.
    EXPECT_TRUE(create_result.recalculated_checksum);

    // Check that our document still exists even though dirty bit was true.
    EXPECT_THAT(proto_log->ReadProto(document_offset),
                IsOkAndHolds(EqualsProto(document)));

    Header header = ReadHeader(filesystem_, file_path_);
    EXPECT_FALSE(header.GetDirtyFlag());
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       PersistToDiskKeepsPersistedDataAndTruncatesExtraData) {
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace1", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace2", "uri2").Build();
  int document1_offset, document2_offset;
  int log_size;

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Write and persist the first proto
    ICING_ASSERT_OK_AND_ASSIGN(document1_offset,
                               proto_log->WriteProto(document1));
    ICING_ASSERT_OK(proto_log->PersistToDisk());

    // Write, but don't explicitly persist the second proto
    ICING_ASSERT_OK_AND_ASSIGN(document2_offset,
                               proto_log->WriteProto(document2));

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));
    ASSERT_THAT(proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    log_size = filesystem_.GetFileSize(file_path_.c_str());
    ASSERT_GT(log_size, 0);

    // PersistToDisk happens implicitly during the destructor.
  }

  {
    // The header rewind position and checksum aren't updated in this "system
    // crash" scenario.

    std::string bad_proto =
        "some incomplete proto that we didn't finish writing before the "
        "system crashed";
    filesystem_.PWrite(file_path_.c_str(), log_size, bad_proto.data(),
                       bad_proto.size());

    // Double check that we actually wrote something to the underlying file
    ASSERT_GT(filesystem_.GetFileSize(file_path_.c_str()), log_size);
  }

  {
    // We can recover, but we have data loss
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_TRUE(create_result.has_data_loss());
    ASSERT_THAT(create_result.data_loss, Eq(DataLoss::PARTIAL));
    ASSERT_FALSE(create_result.recalculated_checksum);

    // Check that everything was persisted across instances
    ASSERT_THAT(proto_log->ReadProto(document1_offset),
                IsOkAndHolds(EqualsProto(document1)));
    ASSERT_THAT(proto_log->ReadProto(document2_offset),
                IsOkAndHolds(EqualsProto(document2)));

    // We correctly rewound to the last good state.
    ASSERT_EQ(log_size, filesystem_.GetFileSize(file_path_.c_str()));
  }
}

TEST_P(PortableFileBackedProtoLogTest, UnsyncedTailChecksumKeepsData) {
  if (!GetParam()) {
    GTEST_SKIP() << "This test is only relevant to the new header format.";
  }

  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();
  DocumentProto document4 =
      DocumentBuilder().SetKey("namespace", "uri4").Build();
  DocumentProto document5 =
      DocumentBuilder().SetKey("namespace", "uri5").Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  EXPECT_FALSE(create_result.has_data_loss());

  // Write 3 protos to the log and call PersistToDisk to make them into log
  // checksummed section. Record the header at this state.
  ICING_ASSERT_OK(proto_log->WriteProto(document1));
  ICING_ASSERT_OK(proto_log->WriteProto(document2));
  ICING_ASSERT_OK(proto_log->WriteProto(document3));
  ICING_ASSERT_OK(proto_log->PersistToDisk());
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Crc32(3230460271u)));

  // Write 2 more protos and keep them in the unsynced tail section (don't call
  // PersistToDisk, UpdateChecksum, or destructor).
  ICING_ASSERT_OK(proto_log->WriteProto(document4));
  ICING_ASSERT_OK(proto_log->WriteProto(document5));
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Crc32(705827520u)));

  // Read the header for the state of doc1-to-doc5.
  Header header_doc1_to_doc5 = ReadHeader(filesystem_, file_path_);

  // Initialize another instance with the same proto file. Thanks to the
  // unsynced tail checksum, document4 and document5 should be kept.
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result2,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  EXPECT_FALSE(create_result2.has_data_loss());

  // Check the overall checksum. Should remain the same.
  EXPECT_THAT(create_result2.proto_log->GetChecksum(),
              IsOkAndHolds(Crc32(705827520u)));

  // Manually check the header. Should remain the same.
  Header header_after = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_after.GetLegacyHeaderChecksum(),
              Eq(header_doc1_to_doc5.GetLegacyHeaderChecksum()));
  EXPECT_THAT(header_after.GetRewindOffset(),
              Eq(header_doc1_to_doc5.GetRewindOffset()));
  EXPECT_THAT(header_after.GetUnsyncedTailChecksum(),
              Eq(header_doc1_to_doc5.GetUnsyncedTailChecksum()));
  EXPECT_THAT(header_after.GetHeaderChecksum(),
              Eq(header_doc1_to_doc5.GetHeaderChecksum()));
}

TEST_P(PortableFileBackedProtoLogTest,
       DirtyBitIsFalseAfterPutAndPersistToDisk) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    DocumentProto document =
        DocumentBuilder().SetKey("namespace1", "uri1").Build();

    // Write and persist the first proto
    ICING_ASSERT_OK_AND_ASSIGN(int64_t document_offset,
                               proto_log->WriteProto(document));
    ICING_ASSERT_OK(proto_log->PersistToDisk());

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document_offset),
                IsOkAndHolds(EqualsProto(document)));
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));

    // We previously persisted to disk so everything should be in a perfect
    // state.
    EXPECT_FALSE(create_result.has_data_loss());
    EXPECT_FALSE(create_result.recalculated_checksum);

    Header header = ReadHeader(filesystem_, file_path_);
    EXPECT_FALSE(header.GetDirtyFlag());
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       DirtyBitIsFalseAfterDeleteAndPersistToDisk) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    DocumentProto document =
        DocumentBuilder().SetKey("namespace1", "uri1").Build();

    // Write, delete, and persist the first proto
    ICING_ASSERT_OK_AND_ASSIGN(int64_t document_offset,
                               proto_log->WriteProto(document));
    ICING_ASSERT_OK(proto_log->EraseProto(document_offset));
    ICING_ASSERT_OK(proto_log->PersistToDisk());

    // The proto has been erased.
    ASSERT_THAT(proto_log->ReadProto(document_offset),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));

    // We previously persisted to disk so everything should be in a perfect
    // state.
    EXPECT_FALSE(create_result.has_data_loss());
    EXPECT_FALSE(create_result.recalculated_checksum);

    Header header = ReadHeader(filesystem_, file_path_);
    EXPECT_FALSE(header.GetDirtyFlag());
  }
}

TEST_P(PortableFileBackedProtoLogTest, DirtyBitIsFalseAfterPutAndDestructor) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    DocumentProto document =
        DocumentBuilder().SetKey("namespace1", "uri1").Build();

    // Write and persist the first proto
    ICING_ASSERT_OK_AND_ASSIGN(int64_t document_offset,
                               proto_log->WriteProto(document));

    // Check that what we read is what we wrote
    ASSERT_THAT(proto_log->ReadProto(document_offset),
                IsOkAndHolds(EqualsProto(document)));

    // PersistToDisk is implicitly called as part of the destructor and
    // PersistToDisk will clear the dirty bit.
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));

    // We previously persisted to disk so everything should be in a perfect
    // state.
    EXPECT_FALSE(create_result.has_data_loss());
    EXPECT_FALSE(create_result.recalculated_checksum);

    Header header = ReadHeader(filesystem_, file_path_);
    EXPECT_FALSE(header.GetDirtyFlag());
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       DirtyBitIsFalseAfterDeleteAndDestructor) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    DocumentProto document =
        DocumentBuilder().SetKey("namespace1", "uri1").Build();

    // Write, delete, and persist the first proto
    ICING_ASSERT_OK_AND_ASSIGN(int64_t document_offset,
                               proto_log->WriteProto(document));
    ICING_ASSERT_OK(proto_log->EraseProto(document_offset));

    // The proto has been erased.
    ASSERT_THAT(proto_log->ReadProto(document_offset),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

    // PersistToDisk is implicitly called as part of the destructor and
    // PersistToDisk will clear the dirty bit.
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));

    // We previously persisted to disk so everything should be in a perfect
    // state.
    EXPECT_FALSE(create_result.has_data_loss());
    EXPECT_FALSE(create_result.recalculated_checksum);

    Header header = ReadHeader(filesystem_, file_path_);
    EXPECT_FALSE(header.GetDirtyFlag());
  }
}

TEST_P(PortableFileBackedProtoLogTest, Iterator) {
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  {
    // Empty iterator
    auto iterator = proto_log->GetIterator();
    ASSERT_THAT(iterator.Advance(),
                StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
  }

  {
    // Iterates through some documents
    ICING_ASSERT_OK(proto_log->WriteProto(document1));
    ICING_ASSERT_OK(proto_log->WriteProto(document2));
    auto iterator = proto_log->GetIterator();
    // 1st proto
    ICING_ASSERT_OK(iterator.Advance());
    ASSERT_THAT(proto_log->ReadProto(iterator.GetOffset()),
                IsOkAndHolds(EqualsProto(document1)));
    // 2nd proto
    ICING_ASSERT_OK(iterator.Advance());
    ASSERT_THAT(proto_log->ReadProto(iterator.GetOffset()),
                IsOkAndHolds(EqualsProto(document2)));
    // Tries to advance
    ASSERT_THAT(iterator.Advance(),
                StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
  }
}

TEST_P(PortableFileBackedProtoLogTest, UpdateChecksum) {
  DocumentProto document = DocumentBuilder().SetKey("namespace", "uri").Build();
  Crc32 checksum;

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    ICING_EXPECT_OK(proto_log->WriteProto(document));

    ICING_ASSERT_OK_AND_ASSIGN(checksum, proto_log->GetChecksum());
    EXPECT_THAT(proto_log->UpdateChecksum(), IsOkAndHolds(Eq(checksum)));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(checksum)));

    // Calling it twice with no changes should get us the same checksum
    EXPECT_THAT(proto_log->UpdateChecksum(), IsOkAndHolds(Eq(checksum)));
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Checksum should be consistent across instances
    ICING_ASSERT_OK_AND_ASSIGN(checksum, proto_log->GetChecksum());
    EXPECT_THAT(proto_log->UpdateChecksum(), IsOkAndHolds(Eq(checksum)));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(checksum)));

    // PersistToDisk shouldn't affect the checksum value
    ICING_EXPECT_OK(proto_log->PersistToDisk());
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(checksum)));

    // Check that modifying the log leads to a different checksum
    ICING_EXPECT_OK(proto_log->WriteProto(document));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Not(Eq(checksum))));
    EXPECT_THAT(proto_log->UpdateChecksum(), IsOkAndHolds(Not(Eq(checksum))));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Not(Eq(checksum))));
  }
}

TEST_P(PortableFileBackedProtoLogTest, EraseProtoShouldSetZero) {
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  // Writes and erases proto
  ICING_ASSERT_OK_AND_ASSIGN(int64_t document1_offset,
                             proto_log->WriteProto(document1));
  ICING_ASSERT_OK(proto_log->EraseProto(document1_offset));

  // Checks if the erased area is set to 0.
  int64_t file_size = filesystem_.GetFileSize(file_path_.c_str());
  ICING_ASSERT_OK_AND_ASSIGN(
      MemoryMappedFile mmapped_file,
      MemoryMappedFile::Create(filesystem_, file_path_,
                               MemoryMappedFile::Strategy::READ_ONLY));

  // document1_offset + sizeof(int) is the start byte of the proto where
  // sizeof(int) is the size of the proto metadata.
  ICING_ASSERT_OK(
      mmapped_file.Remap(document1_offset + sizeof(int), file_size - 1));
  for (size_t i = 0; i < mmapped_file.region_size(); ++i) {
    ASSERT_THAT(mmapped_file.region()[i], Eq(0));
  }
}

TEST_P(PortableFileBackedProtoLogTest, EraseProtoShouldReturnNotFound) {
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  // Writes 2 protos
  ICING_ASSERT_OK_AND_ASSIGN(int64_t document1_offset,
                             proto_log->WriteProto(document1));
  ICING_ASSERT_OK_AND_ASSIGN(int64_t document2_offset,
                             proto_log->WriteProto(document2));

  // Erases the first proto
  ICING_ASSERT_OK(proto_log->EraseProto(document1_offset));

  // The first proto has been erased.
  ASSERT_THAT(proto_log->ReadProto(document1_offset),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  // The second proto should be returned.
  ASSERT_THAT(proto_log->ReadProto(document2_offset),
              IsOkAndHolds(EqualsProto(document2)));
}

TEST_P(PortableFileBackedProtoLogTest, GetChecksumShouldNotUpdateHeader) {
  // This test verifies that GetChecksum() returns the correct checksum after
  // each change, but the header checksum is not updated if UpdateChecksum() or
  // PersistToDisk() is not called.
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  // Sanity check: empty checksum.
  ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(0u))));

  // Put document1 and persist to disk.
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  ICING_ASSERT_OK(proto_log->WriteProto(document1));
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1883553267u))));
  ICING_ASSERT_OK(proto_log->PersistToDisk());
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1883553267u))));

  // Put document2 without calling UpdateChecksum() or PersistToDisk().
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  ICING_ASSERT_OK(proto_log->WriteProto(document2));

  // Read the header.
  Header header_before_get_checksum = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_before_get_checksum.GetLegacyHeaderChecksum(),
              Eq(1460433589u));
  EXPECT_THAT(header_before_get_checksum.GetLogChecksum(), Eq(1883553267u));
  CheckNewChecksums(header_before_get_checksum,
                    /*expected_unsynced_tail_checksum=*/3503817807u,
                    /*expected_header_checksum=*/1080094584u,
                    /*enable_new_header_format=*/GetParam());

  // Since there is a new document (document2) in the unsynced tail, the file is
  // dirty. GetChecksum():
  // - Should return the checksum reflecting the new written proto (document2).
  // - Should not update anything in the header.
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1632103647u))));

  // Manually check the header: although GetChecksum() reflects the latest
  // changes, the header file should remain unchanged.
  Header header_after_get_checksum = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_after_get_checksum.GetLegacyHeaderChecksum(),
              Eq(1460433589u));
  EXPECT_THAT(header_after_get_checksum.GetLogChecksum(), Eq(1883553267u));
  CheckNewChecksums(header_after_get_checksum,
                    /*expected_unsynced_tail_checksum=*/3503817807u,
                    /*expected_header_checksum=*/1080094584u,
                    /*enable_new_header_format=*/GetParam());
  EXPECT_THAT(header_after_get_checksum.GetRewindOffset(),
              Eq(header_before_get_checksum.GetRewindOffset()));

  // Call UpdateChecksum(). Should update the header.
  // - Legacy header checksum should be updated.
  // - Log checksum should be updated.
  // - Rewind offset should be updated.
  ICING_ASSERT_OK(proto_log->UpdateChecksum());
  Header header_after_update_checksum = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_after_update_checksum.GetLegacyHeaderChecksum(),
              Eq(455544443u));
  EXPECT_THAT(header_after_update_checksum.GetLogChecksum(), Eq(1632103647u));
  // If the flag is enabled, the unsynced tail checksum and header checksum
  // should be updated. Since we merged unsynced tail section into log
  // checksummed section, the unsynced tail checksum should be updated to 0.
  CheckNewChecksums(header_after_update_checksum,
                    /*expected_unsynced_tail_checksum=*/0u,
                    /*expected_header_checksum=*/2472053009u,
                    /*enable_new_header_format=*/GetParam());
  EXPECT_THAT(header_after_update_checksum.GetRewindOffset(),
              Ne(header_after_get_checksum.GetRewindOffset()));
}

TEST_P(PortableFileBackedProtoLogTest,
       GetChecksumShouldBeCorrectWithWriteProto) {
  // This test verifies that GetChecksum() returns the correct checksum after
  // each change, even if UpdateChecksum() or PersistToDisk() is not called.
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              compression_threshold_bytes_, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  ICING_ASSERT_OK(proto_log->WriteProto(document1));
  // Although we didn't call UpdateChecksum() or PersistToDisk(), GetChecksum()
  // should return the value reflecting the new written proto (document1).
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1883553267u))));

  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  ICING_ASSERT_OK(proto_log->WriteProto(document2));

  const Crc32 kChecksumWithDocument2(1632103647u);

  // Although we didn't call UpdateChecksum() or PersistToDisk(), GetChecksum()
  // should return the value reflecting the new written proto (document1,
  // document2).
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(kChecksumWithDocument2));

  // Call PersistToDisk.
  ICING_ASSERT_OK(proto_log->PersistToDisk());
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(kChecksumWithDocument2));

  // Read the header.
  Header header_before_write_proto = ReadHeader(filesystem_, file_path_);

  // Put document3 without calling UpdateChecksum() or PersistToDisk().
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();
  ICING_ASSERT_OK(proto_log->WriteProto(document3));

  const Crc32 kChecksumWithDocument3(3230460271u);
  // Although we didn't call UpdateChecksum() or PersistToDisk(), GetChecksum()
  // should return the value reflecting the new written proto (document3).
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(kChecksumWithDocument3));

  // Manually check the header.
  Header header_after_write_proto = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_after_write_proto.GetLegacyHeaderChecksum(),
              Eq(header_before_write_proto.GetLegacyHeaderChecksum()));
  EXPECT_THAT(header_after_write_proto.GetRewindOffset(),
              Eq(header_before_write_proto.GetRewindOffset()));
  EXPECT_THAT(header_after_write_proto.GetLogChecksum(),
              Eq(header_before_write_proto.GetLogChecksum()));
  if (GetParam()) {
    // If the flag is enabled, when writing document3, the unsynced tail
    // checksum and header checksum should be updated.
    EXPECT_THAT(header_after_write_proto.GetUnsyncedTailChecksum(),
                Ne(header_before_write_proto.GetUnsyncedTailChecksum()));
    EXPECT_THAT(header_after_write_proto.GetHeaderChecksum(),
                Ne(header_before_write_proto.GetHeaderChecksum()));

    // Create another instance of the same proto log.
    // - Nothing will be discarded thanks to the unsynced tail checksum.
    // - GetChecksum() should return the same checksum after writing document3.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result2,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log2 = std::move(create_result2.proto_log);
    EXPECT_FALSE(create_result2.has_data_loss());
    EXPECT_THAT(proto_log2->GetChecksum(),
                IsOkAndHolds(kChecksumWithDocument3));
  } else {
    // If the flag is disabled, the unsynced tail checksum and header checksum
    // should remain 0.
    EXPECT_THAT(header_after_write_proto.GetUnsyncedTailChecksum(), Eq(0u));
    EXPECT_THAT(header_after_write_proto.GetHeaderChecksum(), Eq(0u));

    // Create another instance of the same proto log.
    // - document3 will be discarded since it is after the rewind offset.
    // - GetChecksum() will return the old one before writing document3.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result2,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log2 = std::move(create_result2.proto_log);
    EXPECT_THAT(create_result2.data_loss, Eq(DataLoss::PARTIAL));
    EXPECT_THAT(proto_log2->GetChecksum(),
                IsOkAndHolds(kChecksumWithDocument2));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       GetChecksumShouldBeCorrectWithErasedProtoAfterRewindOffset) {
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();
  DocumentProto document4 =
      DocumentBuilder().SetKey("namespace", "uri4").Build();
  DocumentProto document5 =
      DocumentBuilder().SetKey("namespace", "uri5").Build();
  DocumentProto document6 =
      DocumentBuilder().SetKey("namespace", "uri6").Build();

  {
    // Write 3 protos.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Sanity check: empty checksum.
    ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(0u))));

    // Writes 3 protos
    ICING_ASSERT_OK(proto_log->WriteProto(document1));
    ICING_ASSERT_OK(proto_log->WriteProto(document2));
    ICING_ASSERT_OK(proto_log->WriteProto(document3));
    // Although we didn't call UpdateChecksum() or PersistToDisk(),
    // GetChecksum() should return the value reflecting the new written proto.
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));
  }  // New checksum is written to the header and flushed in destructor.

  {
    // Erase data after the rewind position.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Sanity check that the checksum is identical to the previous one.
    ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));

    // Write 2 more documents to the unsynced tail.
    ICING_ASSERT_OK_AND_ASSIGN(int64_t document4_offset,
                               proto_log->WriteProto(document4));
    ICING_ASSERT_OK(proto_log->WriteProto(document5));
    // Although we didn't call UpdateChecksum() or PersistToDisk(),
    // GetChecksum() should return the value reflecting the new written proto.
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(705827520u))));

    // Erases the 4th proto that is after the rewind position. GetChecksum()
    // should return the new checksum reflecting the erased proto.
    ICING_ASSERT_OK(proto_log->EraseProto(document4_offset));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1052805596u))));
  }  // New checksum is written to the header and flushed in destructor.

  {
    // A successful creation means that the checksum matches.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    EXPECT_FALSE(create_result.has_data_loss());

    // The checksum should match the one from the previous case.
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1052805596u))));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       GetChecksumShouldBeCorrectWithErasedProtoBeforeRewindOffset) {
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();
  DocumentProto document4 =
      DocumentBuilder().SetKey("namespace", "uri4").Build();
  DocumentProto document5 =
      DocumentBuilder().SetKey("namespace", "uri5").Build();
  DocumentProto document6 =
      DocumentBuilder().SetKey("namespace", "uri6").Build();

  int64_t document2_offset;
  int64_t document3_offset;
  {
    // Write 3 protos.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Sanity check: empty checksum.
    ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(0u))));

    // Writes 3 protos
    ICING_ASSERT_OK(proto_log->WriteProto(document1));
    ICING_ASSERT_OK_AND_ASSIGN(document2_offset,
                               proto_log->WriteProto(document2));
    ICING_ASSERT_OK_AND_ASSIGN(document3_offset,
                               proto_log->WriteProto(document3));
    // Although we didn't call UpdateChecksum() or PersistToDisk(),
    // GetChecksum() should return the value reflecting the new written proto.
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));

    // Call PersistToDisk() so that the unsynced tail is flushed and becomes
    // synced section.
    ICING_ASSERT_OK(proto_log->PersistToDisk());
  }

  {
    // Erase data before the rewind position.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Sanity check that the checksum is identical to the previous one.
    ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));

    // Erases the 2nd proto that is before the rewind position.
    // GetChecksum() should return the new checksum reflecting the erased proto.
    ICING_ASSERT_OK(proto_log->EraseProto(document2_offset));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1845730629u))));
  }  // New checksum is written to the header and flushed in destructor.

  {
    // Append data to the unsynced tail and erase data before the rewind
    // position.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Sanity check that the checksum is identical to the previous one.
    ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1845730629u))));

    // Append a new document which is after the rewind position.
    ICING_ASSERT_OK(proto_log->WriteProto(document6));

    // Erases the 3rd proto that is before the rewind position.
    // GetChecksum() should return the new checksum reflecting document6 and the
    // erased proto.
    ICING_ASSERT_OK(proto_log->EraseProto(document3_offset));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3659111045u))));
  }  // New checksum is written to the header and flushed in destructor.

  {
    // A successful creation means that the checksum matches.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                compression_threshold_bytes_, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    EXPECT_FALSE(create_result.has_data_loss());

    // The checksum should match the one from the previous case.
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3659111045u))));
  }
}

TEST_P(PortableFileBackedProtoLogTest, ErasedProtoAfterRewindOffset) {
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();
  DocumentProto document4 =
      DocumentBuilder().SetKey("namespace", "uri4").Build();
  DocumentProto document5 =
      DocumentBuilder().SetKey("namespace", "uri5").Build();

  {
    // Write 3 protos.
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Sanity check: empty checksum.
    ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(0u))));

    // Writes 3 protos
    ICING_ASSERT_OK(proto_log->WriteProto(document1));
    ICING_ASSERT_OK(proto_log->WriteProto(document2));
    ICING_ASSERT_OK(proto_log->WriteProto(document3));
    // Although we didn't call UpdateChecksum() or PersistToDisk(),
    // GetChecksum() should return the value reflecting the new written proto.
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));
  }  // New checksum is written to the header and flushed in destructor.

  // Erase data after the rewind position.
  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              /*compression_threshold_bytes_in=*/0, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  // Sanity check that the checksum is identical to the previous one.
  ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));

  // Write 2 more documents to the unsynced tail.
  ICING_ASSERT_OK_AND_ASSIGN(int64_t document4_offset,
                             proto_log->WriteProto(document4));
  ICING_ASSERT_OK(proto_log->WriteProto(document5));
  // Although we didn't call UpdateChecksum() or PersistToDisk(),
  // GetChecksum() should return the value reflecting the new written proto.
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(705827520u))));

  // Read the header.
  Header header_before_erase = ReadHeader(filesystem_, file_path_);

  // Erases the 4th proto that is after the rewind position. GetChecksum()
  // should return the new checksum reflecting the erased proto.
  ICING_ASSERT_OK(proto_log->EraseProto(document4_offset));
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1052805596u))));

  // Manually check the header.
  Header header_after_erase = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_after_erase.GetLegacyHeaderChecksum(),
              Eq(header_before_erase.GetLegacyHeaderChecksum()));
  EXPECT_THAT(header_after_erase.GetRewindOffset(),
              Eq(header_before_erase.GetRewindOffset()));
  EXPECT_THAT(header_after_erase.GetLogChecksum(),
              Eq(header_before_erase.GetLogChecksum()));
  if (GetParam()) {
    // If the flag is enabled, when erasing document4, the unsynced tail
    // checksum and header checksum should be updated.
    EXPECT_THAT(header_after_erase.GetUnsyncedTailChecksum(),
                Ne(header_before_erase.GetUnsyncedTailChecksum()));
    EXPECT_THAT(header_after_erase.GetHeaderChecksum(),
                Ne(header_before_erase.GetHeaderChecksum()));
  } else {
    // If the flag is disabled, the unsynced tail checksum and header checksum
    // should remain 0.
    EXPECT_THAT(header_after_erase.GetUnsyncedTailChecksum(), Eq(0u));
    EXPECT_THAT(header_after_erase.GetHeaderChecksum(), Eq(0u));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       ErasedProtoBeforeRewindOffsetShouldUpdateHeader) {
  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
      PortableFileBackedProtoLog<DocumentProto>::Create(
          &filesystem_, file_path_,
          PortableFileBackedProtoLog<DocumentProto>::Options(
              compress_, max_proto_size_, compression_level_,
              /*compression_threshold_bytes_in=*/0, compression_mem_level_,
              enable_smaller_decompression_buffer_size_,
              /*enable_new_header_format_in=*/GetParam())));
  auto proto_log = std::move(create_result.proto_log);
  ASSERT_FALSE(create_result.has_data_loss());

  // Sanity check: empty checksum.
  ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(0u))));

  // Writes 3 protos
  ICING_ASSERT_OK(proto_log->WriteProto(document1));
  ICING_ASSERT_OK_AND_ASSIGN(int64_t document2_offset,
                             proto_log->WriteProto(document2));
  ICING_ASSERT_OK(proto_log->WriteProto(document3));
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));

  // Persist to disk in order to make document1 to document3 in the log
  // checksummed section.
  ICING_ASSERT_OK(proto_log->PersistToDisk());

  // Read the header.
  Header header_before_erase = ReadHeader(filesystem_, file_path_);

  // Erases the 2nd proto that is before the rewind position. GetChecksum()
  // should return the new checksum reflecting the erased proto.
  ICING_ASSERT_OK(proto_log->EraseProto(document2_offset));
  EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1845730629u))));

  // Manually check the header:
  // - Log checksum is updated because the log checksummed section was updated.
  // - Legacy header checksum is updated because log checksum was updated.
  // - RewindOffset should remain the same.
  // - (Only if enable_new_header_format flag is true) Header checksum is
  //   updated because the header was updated.
  Header header_after_erase = ReadHeader(filesystem_, file_path_);
  EXPECT_THAT(header_after_erase.GetLegacyHeaderChecksum(),
              Ne(header_before_erase.GetLegacyHeaderChecksum()));
  EXPECT_THAT(header_after_erase.GetRewindOffset(),
              Eq(header_before_erase.GetRewindOffset()));
  EXPECT_THAT(header_after_erase.GetLogChecksum(),
              Ne(header_before_erase.GetLogChecksum()));
  if (GetParam()) {
    EXPECT_THAT(header_after_erase.GetHeaderChecksum(),
                Ne(header_before_erase.GetHeaderChecksum()));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       ReadWriteHeaderShouldIncludePaddingBytes) {
  // The header should have some padding bytes to make it 8-byte aligned.
  static_assert(Header::kLegacyHeaderSectionSize -
                    Header::kLegacyHeaderSectionPaddingOffset >
                0);
  std::string bytes = "abc";

  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "uri1").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "uri2").Build();
  DocumentProto document3 =
      DocumentBuilder().SetKey("namespace", "uri3").Build();

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Sanity check: empty checksum.
    ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(0u))));

    // Writes 2 protos
    ICING_ASSERT_OK(proto_log->WriteProto(document1));
    ICING_ASSERT_OK(proto_log->WriteProto(document2));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(1632103647u))));
    // Persist to disk.
    ICING_ASSERT_OK(proto_log->PersistToDisk());

    // Overwrite the padding bytes with some random bytes.
    Header* header = proto_log->header();
    memcpy(reinterpret_cast<char*>(header) +
               Header::kLegacyHeaderSectionPaddingOffset,
           bytes.data(),
           Header::kLegacyHeaderSectionSize -
               Header::kLegacyHeaderSectionPaddingOffset);

    // Add 1 more proto. It is a hack to make the proto log dirty and make the
    // next PersistToDisk take effect.
    ICING_ASSERT_OK(proto_log->WriteProto(document3));
    EXPECT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));
    ICING_ASSERT_OK(proto_log->PersistToDisk());

    // Manually check the header: the padding bytes contain the overwritten
    // values.
    // Ensures that write header operation also includes the padding bytes.
    Header header_after = ReadHeader(filesystem_, file_path_);
    EXPECT_THAT(std::string(reinterpret_cast<const char*>(&header_after) +
                                Header::kLegacyHeaderSectionPaddingOffset,
                            Header::kLegacyHeaderSectionSize -
                                Header::kLegacyHeaderSectionPaddingOffset),
                Eq(bytes));
  }

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        PortableFileBackedProtoLog<DocumentProto>::CreateResult create_result,
        PortableFileBackedProtoLog<DocumentProto>::Create(
            &filesystem_, file_path_,
            PortableFileBackedProtoLog<DocumentProto>::Options(
                compress_, max_proto_size_, compression_level_,
                /*compression_threshold_bytes_in=*/0, compression_mem_level_,
                enable_smaller_decompression_buffer_size_,
                /*enable_new_header_format_in=*/GetParam())));
    auto proto_log = std::move(create_result.proto_log);
    ASSERT_FALSE(create_result.has_data_loss());

    // Sanity check that the checksum is identical to the previous one.
    ASSERT_THAT(proto_log->GetChecksum(), IsOkAndHolds(Eq(Crc32(3230460271u))));

    // Verify that read header operation includes the padding bytes.
    Header* header = proto_log->header();
    EXPECT_THAT(std::string(reinterpret_cast<const char*>(header) +
                                Header::kLegacyHeaderSectionPaddingOffset,
                            Header::kLegacyHeaderSectionSize -
                                Header::kLegacyHeaderSectionPaddingOffset),
                Eq(bytes));
  }
}

TEST_P(PortableFileBackedProtoLogTest,
       CalculateLegacyHeaderChecksumShouldIncludePadding) {
  // The legacy header should have some padding bytes to make it 8-byte aligned.
  static_assert(Header::kLegacyHeaderSectionSize -
                    Header::kLegacyHeaderSectionPaddingOffset >
                0);
  std::string bytes = "abc";

  // Create a header instance and set all bytes to 1 for the header.
  // Note that -1 means 0b111111...111.
  Header header;
  memset(&header, -1, sizeof(Header));
  EXPECT_THAT(header.CalculateLegacyHeaderChecksum(), Eq(2132597986u));

  // Set different bytes to the padding section. Verify that the legacy header
  // checksum reflects the padding byte changes.
  memcpy(reinterpret_cast<char*>(&header) +
             Header::kLegacyHeaderSectionPaddingOffset,
         bytes.data(),
         Header::kLegacyHeaderSectionSize -
             Header::kLegacyHeaderSectionPaddingOffset);
  EXPECT_THAT(header.CalculateLegacyHeaderChecksum(), Eq(3049742880u));
}

INSTANTIATE_TEST_SUITE_P(PortableFileBackedProtoLogTest,
                         PortableFileBackedProtoLogTest,
                         testing::Values(true, false));

}  // namespace
}  // namespace lib
}  // namespace icing
