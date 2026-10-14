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

#include "icing/file/file-backed-proto.h"

#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::Le;
using ::testing::Not;
using ::testing::Pointee;
using ::testing::SizeIs;

namespace icing {
namespace lib {
namespace {

class FileBackedProtoTest : public ::testing::Test {
 protected:
  void SetUp() override { filename_ = GetTestTempDir() + "/schema.pb"; }

  void TearDown() override { filesystem_.DeleteFile(filename_.c_str()); }

  Filesystem filesystem_;
  std::string filename_;
};

TEST_F(FileBackedProtoTest, SimpleReadWriteTest) {
  DocumentProto document =
      DocumentBuilder().SetKey("namespace", "google.com").Build();

  FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
  ICING_ASSERT_OK(file_proto.Write(std::make_unique<DocumentProto>(document)));
  EXPECT_THAT(file_proto.Read(), IsOkAndHolds(Pointee(EqualsProto(document))));
  // Multiple reads work.
  EXPECT_THAT(file_proto.Read(), IsOkAndHolds(Pointee(EqualsProto(document))));
  EXPECT_THAT(file_proto.Read(), IsOkAndHolds(Pointee(EqualsProto(document))));
}

TEST_F(FileBackedProtoTest, GetChecksum) {
  DocumentProto document =
      DocumentBuilder().SetKey("namespace", "google.com").Build();

  FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
  EXPECT_THAT(file_proto.GetChecksum(), IsOkAndHolds(Crc32(0)));

  ICING_ASSERT_OK(file_proto.Write(std::make_unique<DocumentProto>(document)));
  EXPECT_THAT(file_proto.GetChecksum(), IsOkAndHolds(Crc32(385085217)));
}

TEST_F(FileBackedProtoTest, GetChecksumAcrossInstances) {
  DocumentProto document =
      DocumentBuilder().SetKey("namespace", "google.com").Build();

  {
    FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
    EXPECT_THAT(file_proto.GetChecksum(), IsOkAndHolds(Crc32(0)));
  }

  {
    FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
    EXPECT_THAT(file_proto.GetChecksum(), IsOkAndHolds(Crc32(0)));
    ICING_ASSERT_OK(
        file_proto.Write(std::make_unique<DocumentProto>(document)));
    EXPECT_THAT(file_proto.GetChecksum(), IsOkAndHolds(Crc32(385085217)));
  }

  {
    FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
    EXPECT_THAT(file_proto.GetChecksum(), IsOkAndHolds(Crc32(385085217)));
  }
}

TEST_F(FileBackedProtoTest, DataPersistsAcrossMultipleInstancesTest) {
  DocumentProto document =
      DocumentBuilder().SetKey("namespace", "google.com").Build();

  {
    FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
    EXPECT_THAT(file_proto.Read(), Not(IsOk()));  // Nothing to read.

    ICING_ASSERT_OK(
        file_proto.Write(std::make_unique<DocumentProto>(document)));
    EXPECT_THAT(file_proto.Read(),
                IsOkAndHolds(Pointee(EqualsProto(document))));
  }
  {
    // Different instance of FileBackedProto.
    FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
    EXPECT_THAT(file_proto.Read(),
                IsOkAndHolds(Pointee(EqualsProto(document))));
  }
}

TEST_F(FileBackedProtoTest, MultipleUpdatesToProtoTest) {
  DocumentProto googleProto =
      DocumentBuilder().SetKey("namespace", "google.com").Build();
  DocumentProto youtubeProto =
      DocumentBuilder().SetKey("namespace", "youtube.com").Build();
  DocumentProto wazeProto =
      DocumentBuilder().SetKey("namespace", "waze.com").Build();

  {
    FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
    ICING_ASSERT_OK(
        file_proto.Write(std::make_unique<DocumentProto>(googleProto)));
    EXPECT_THAT(file_proto.Read(),
                IsOkAndHolds(Pointee(EqualsProto(googleProto))));

    ICING_ASSERT_OK(
        file_proto.Write(std::make_unique<DocumentProto>(youtubeProto)));
    EXPECT_THAT(file_proto.Read(),
                IsOkAndHolds(Pointee(EqualsProto(youtubeProto))));
  }
  {
    // Different instance of FileBackedProto.
    FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
    EXPECT_THAT(file_proto.Read(),
                IsOkAndHolds(Pointee(EqualsProto(youtubeProto))));

    ICING_ASSERT_OK(
        file_proto.Write(std::make_unique<DocumentProto>(wazeProto)));
    EXPECT_THAT(file_proto.Read(),
                IsOkAndHolds(Pointee(EqualsProto(wazeProto))));

    ICING_ASSERT_OK(
        file_proto.Write(std::make_unique<DocumentProto>(googleProto)));
    EXPECT_THAT(file_proto.Read(),
                IsOkAndHolds(Pointee(EqualsProto(googleProto))));
  }
}

TEST_F(FileBackedProtoTest, InvalidFilenameTest) {
  DocumentProto document =
      DocumentBuilder().SetKey("namespace", "google.com").Build();

  FileBackedProto<DocumentProto> file_proto(filesystem_, "");
  EXPECT_THAT(file_proto.Read(), Not(IsOk()));
  EXPECT_THAT(file_proto.Write(std::make_unique<DocumentProto>(document)),
              Not(IsOk()));
}

TEST_F(FileBackedProtoTest, FileCorruptionTest) {
  DocumentProto document =
      DocumentBuilder().SetKey("namespace", "google.com").Build();

  {
    FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
    ICING_ASSERT_OK(
        file_proto.Write(std::make_unique<DocumentProto>(document)));
    EXPECT_THAT(file_proto.Read(),
                IsOkAndHolds(Pointee(EqualsProto(document))));
  }

  document.set_uri("g00gle.com");
  std::string document_str = document.SerializeAsString();
  filesystem_.PWrite(filename_.c_str(),
                     /*offset=*/sizeof(FileBackedProto<DocumentProto>::Header),
                     document_str.data(), document_str.size());

  FileBackedProto<DocumentProto> file_proto(filesystem_, filename_);
  EXPECT_THAT(file_proto.Read(), Not(IsOk()));
}

TEST_F(FileBackedProtoTest, MaxProtoSizeReadWrite) {
  // Construct a schema proto with max size. This test ensures that the max
  // proto size and max file size are computed and checked correctly between
  // read and write methods.
  std::string type_a_description(1048534, 'a');
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("TypeA")
                       .SetDescription(std::move(type_a_description))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  std::string schema_str = schema.SerializeAsString();
  ASSERT_THAT(schema_str, SizeIs(FileBackedProto<SchemaProto>::kMaxProtoSize));
  ASSERT_THAT(schema_str.size() + sizeof(FileBackedProto<SchemaProto>::Header),
              Le(FileBackedProto<SchemaProto>::kMaxFileSize));

  {
    FileBackedProto<SchemaProto> file_proto(filesystem_, filename_);
    EXPECT_THAT(file_proto.Write(std::make_unique<SchemaProto>(schema)),
                IsOk());
    EXPECT_THAT(file_proto.Read(), IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  {
    // Re-initialize the instance of FileBackedProto.
    FileBackedProto<SchemaProto> file_proto(filesystem_, filename_);
    EXPECT_THAT(file_proto.Read(), IsOkAndHolds(Pointee(EqualsProto(schema))));
  }
}

TEST_F(FileBackedProtoTest, ExceedingMaxProtoSizeShouldFail) {
  // Construct a schema proto with size larger than kMaxProtoSize. This test
  // ensures that the max proto size and max file size are computed and checked
  // correctly between read and write methods.
  std::string type_a_description(1048535, 'a');
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("TypeA")
                       .SetDescription(std::move(type_a_description))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  std::string schema_str = schema.SerializeAsString();
  ASSERT_THAT(schema_str,
              SizeIs(FileBackedProto<SchemaProto>::kMaxProtoSize + 1));

  FileBackedProto<SchemaProto> file_proto(filesystem_, filename_);
  EXPECT_THAT(file_proto.Write(std::make_unique<SchemaProto>(schema)),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("New proto too large")));
}

}  // namespace
}  // namespace lib
}  // namespace icing
