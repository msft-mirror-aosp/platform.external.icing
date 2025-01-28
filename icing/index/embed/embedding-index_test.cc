// Copyright (C) 2024 Google LLC
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

#include "icing/index/embed/embedding-index.h"

#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/quantizer.h"
#include "icing/index/hit/hit.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::FloatNear;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Pointwise;
using ::testing::Test;

static constexpr SectionId kSectionIdQuantizedEmbedding = 2;
static constexpr float kEpsQuantized = 0.01f;

class EmbeddingIndexTest : public Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    test_dir_ = GetTestTempDir() + "/icing";
    embedding_index_dir_ = test_dir_ + "/embedding_index";
    document_store_dir_ = test_dir_ + "/document_store";
    schema_store_dir_ = test_dir_ + "/schema_store";
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(document_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &clock_, feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, document_store_dir_, &clock_,
                              schema_store_.get(), feature_flags_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*pre_mapping_fbv=*/false,
                              /*use_persistent_hash_map=*/true,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDefaultCompressionLevel,
                              /*initialize_stats=*/nullptr));
    document_store_ = std::move(create_result.document_store);

    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_, &clock_,
                               feature_flags_.get()));

    ICING_ASSERT_OK(schema_store_->SetSchema(
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("type")
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("prop1")
                            .SetDataTypeVector(EMBEDDING_INDEXING_LINEAR_SEARCH)
                            .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("prop2")
                            .SetDataTypeVector(EMBEDDING_INDEXING_LINEAR_SEARCH)
                            .SetCardinality(CARDINALITY_OPTIONAL))
                    // Quantized embedding
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("prop3")
                            .SetDataTypeVector(EMBEDDING_INDEXING_LINEAR_SEARCH,
                                               QUANTIZATION_TYPE_QUANTIZE_8_BIT)
                            .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build(),
        /*ignore_errors_and_delete_documents=*/false));
    ICING_ASSERT_OK(document_store_->Put(
        DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
    ICING_ASSERT_OK(document_store_->Put(
        DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
    ICING_ASSERT_OK(document_store_->Put(
        DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    embedding_index_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  libtextclassifier3::StatusOr<bool> IndexContainsMetadataOnly() {
    std::vector<std::string> sub_dirs;
    if (!filesystem_.ListDirectory(embedding_index_dir_.c_str(), /*exclude=*/{},
                                   /*recursive=*/true, &sub_dirs)) {
      return absl_ports::InternalError("Failed to list directory");
    }
    return sub_dirs.size() == 1 && sub_dirs[0] == "metadata";
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  IcingFilesystem icing_filesystem_;
  std::string test_dir_;
  std::string embedding_index_dir_;
  std::string schema_store_dir_;
  std::string document_store_dir_;
  Clock clock_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<EmbeddingIndex> embedding_index_;
};

TEST_F(EmbeddingIndexTest, EmptyIndexContainsMetadataOnly) {
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
}

TEST_F(EmbeddingIndexTest, InitializationShouldFailWithNullPointer) {
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";

  EXPECT_THAT(EmbeddingIndex::Create(nullptr, embedding_index_dir, &clock_,
                                     feature_flags_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(EmbeddingIndex::Create(&filesystem_, embedding_index_dir, nullptr,
                                     feature_flags_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(EmbeddingIndexTest,
       InitializationShouldFailWithoutPersistToDiskOrDestruction) {
  // 1. Create index and confirm that data was properly added.
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get()));

  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index->CommitBufferToIndex());
  embedding_index->set_last_added_document_id(0);

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index->last_added_document_id(), 0);
  // GetChecksum should succeed without updating the checksum.
  ICING_EXPECT_OK(embedding_index->GetChecksum());

  // 2. Try to create another index with the same directory. This should fail
  // due to checksum mismatch.
  EXPECT_THAT(EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                                     feature_flags_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  embedding_index.reset();
  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest, InitializationShouldSucceedWithUpdateChecksums) {
  // 1. Create index and confirm that data was properly added.
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get()));

  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index->CommitBufferToIndex());
  embedding_index->set_last_added_document_id(0);

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index->last_added_document_id(), 0);

  // 2. Update checksums to reflect the new content.
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 crc, embedding_index->GetChecksum());
  EXPECT_THAT(embedding_index->UpdateChecksums(), IsOkAndHolds(Eq(crc)));
  EXPECT_THAT(embedding_index->GetChecksum(), IsOkAndHolds(Eq(crc)));

  // 3. Create another index and confirm that the data is still there.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index_two,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get()));

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_two.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_two.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_two->last_added_document_id(), 0);

  embedding_index.reset();
  embedding_index_two.reset();
  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest, InitializationShouldSucceedWithPersistToDisk) {
  // 1. Create index and confirm that data was properly added.
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get()));

  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index->CommitBufferToIndex());
  embedding_index->set_last_added_document_id(0);

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index->last_added_document_id(), 0);

  // 2. Update checksums to reflect the new content.
  ICING_EXPECT_OK(embedding_index->PersistToDisk());

  // 3. Create another index and confirm that the data is still there.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index_two,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get()));

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_two.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_two.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_two->last_added_document_id(), 0);

  embedding_index.reset();
  embedding_index_two.reset();
  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest, GetEmbeddingVectorShouldFailWhenOutOfRange) {
  BasicHit basic_hit(/*section_id=*/0, /*document_id=*/0);
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(basic_hit, vector,
                                                    QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  EmbeddingHit embedding_hit(basic_hit, /*location=*/0);
  uint32_t dimension = 3;
  ICING_ASSERT_OK(
      embedding_index_->GetEmbeddingVector(embedding_hit, dimension));
  EXPECT_THAT(
      embedding_index_->GetEmbeddingVector(embedding_hit, dimension + 1),
      StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
}

TEST_F(EmbeddingIndexTest,
       GetQuantizedEmbeddingVectorShouldFailWhenOutOfRange) {
  BasicHit basic_hit(kSectionIdQuantizedEmbedding, /*document_id=*/0);
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      basic_hit, vector, QUANTIZATION_TYPE_QUANTIZE_8_BIT));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  EmbeddingHit embedding_hit(basic_hit, /*location=*/0);
  uint32_t dimension = 3;
  ICING_ASSERT_OK(
      embedding_index_->GetQuantizedEmbeddingVector(embedding_hit, dimension));
  EXPECT_THAT(embedding_index_->GetQuantizedEmbeddingVector(embedding_hit,
                                                            dimension + 1),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
}

TEST_F(EmbeddingIndexTest, AddSingleEmbedding) {
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, AddSingleQuantizedEmbedding) {
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0), vector,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EmbeddingHit hit(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                   /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     hit,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, AddMultipleEmbeddingsInTheSameSection) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/3))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, AddMultipleQuantizedEmbeddingsInTheSameSection) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EmbeddingHit hit1(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                    /*location=*/0);
  EmbeddingHit hit2(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                    /*location=*/3 + sizeof(Quantizer));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(hit1, hit2)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(2 * (3 + sizeof(Quantizer))));  // Two quantized vectors
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     hit1, /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     hit2, /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {-0.1, -0.2, -0.3})));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, HitsWithLowerSectionIdReturnedFirst) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/5, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/2, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/2, /*document_id=*/0),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/5, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, HitsWithHigherDocumentIdReturnedFirst) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);
}

TEST_F(EmbeddingIndexTest, AddEmbeddingsFromDifferentModels) {
  PropertyProto::VectorProto vector1 = CreateVector("model1", {0.1, 0.2});
  PropertyProto::VectorProto vector2 =
      CreateVector("model2", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                        /*model_signature=*/"model1"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model2"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/2))));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(
                  embedding_index_.get(),
                  /*dimension=*/5, /*model_signature=*/"non-existent-model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest,
       AddEmbeddingsWithSameSignatureButDifferentDimension) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/2))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, ClearIndex) {
  // Loop the same logic twice to make sure that clear works as expected, and
  // the index is still valid after clearing.
  for (int i = 0; i < 2; i++) {
    PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
    PropertyProto::VectorProto vector2 =
        CreateVector("model", {-0.1, -0.2, -0.3});
    PropertyProto::VectorProto vector3 = CreateVector("model", {0.4, 0.5, 0.6});

    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/1, /*document_id=*/0), vector1,
        QUANTIZATION_TYPE_NONE));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/2, /*document_id=*/1), vector2,
        QUANTIZATION_TYPE_NONE));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2), vector3,
        QUANTIZATION_TYPE_QUANTIZE_8_BIT));
    ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
    embedding_index_->set_last_added_document_id(2);

    EmbeddingHit hit1(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2),
                      /*location=*/0);
    EmbeddingHit hit2(BasicHit(/*section_id=*/2, /*document_id=*/1),
                      /*location=*/3);
    EmbeddingHit hit3(BasicHit(/*section_id=*/1, /*document_id=*/0),
                      /*location=*/0);

    EXPECT_THAT(
        GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                  /*model_signature=*/"model"),
        IsOkAndHolds(ElementsAre(hit1, hit2, hit3)));
    EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
                ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
    EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
                Eq(3 + sizeof(Quantizer)));
    EXPECT_THAT(
        GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                       hit1,
                                                       /*dimension=*/3),
        IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector3.values())));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 2);
    EXPECT_FALSE(embedding_index_->is_empty());
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(false));

    // Check that clear works as expected.
    ICING_ASSERT_OK(embedding_index_->Clear());
    EXPECT_TRUE(embedding_index_->is_empty());
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
    EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
                IsEmpty());
    EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(), Eq(0));
    EXPECT_EQ(embedding_index_->last_added_document_id(), kInvalidDocumentId);
  }
}

TEST_F(EmbeddingIndexTest, DiscardIndex) {
  // Loop the same logic twice to make sure that Discard works as expected, and
  // the index is still valid after discarding.
  for (int i = 0; i < 2; i++) {
    PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
    PropertyProto::VectorProto vector2 =
        CreateVector("model", {-0.1, -0.2, -0.3});
    PropertyProto::VectorProto vector3 = CreateVector("model", {0.4, 0.5, 0.6});

    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/1, /*document_id=*/0), vector1,
        QUANTIZATION_TYPE_NONE));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/2, /*document_id=*/1), vector2,
        QUANTIZATION_TYPE_NONE));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2), vector3,
        QUANTIZATION_TYPE_QUANTIZE_8_BIT));
    ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
    embedding_index_->set_last_added_document_id(2);

    EmbeddingHit hit1(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2),
                      /*location=*/0);
    EmbeddingHit hit2(BasicHit(/*section_id=*/2, /*document_id=*/1),
                      /*location=*/3);
    EmbeddingHit hit3(BasicHit(/*section_id=*/1, /*document_id=*/0),
                      /*location=*/0);
    EXPECT_THAT(
        GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                  /*model_signature=*/"model"),
        IsOkAndHolds(ElementsAre(hit1, hit2, hit3)));
    EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
                ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
    EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
                Eq(3 + sizeof(Quantizer)));
    EXPECT_THAT(
        GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                       hit1,
                                                       /*dimension=*/3),
        IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector3.values())));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 2);
    EXPECT_FALSE(embedding_index_->is_empty());
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(false));

    // Check that Discard works as expected.
    embedding_index_.reset();
    EmbeddingIndex::Discard(filesystem_, embedding_index_dir_);
    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_, &clock_,
                               feature_flags_.get()));
    EXPECT_TRUE(embedding_index_->is_empty());
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
    EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
                IsEmpty());
    EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(), Eq(0));
    EXPECT_EQ(embedding_index_->last_added_document_id(), kInvalidDocumentId);
  }
}

TEST_F(EmbeddingIndexTest, EmptyCommitIsOk) {
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(), Eq(0));
}

TEST_F(EmbeddingIndexTest, MultipleCommits) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
}

TEST_F(EmbeddingIndexTest,
       InvalidCommit_SectionIdCanOnlyDecreaseForSingleDocument) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE));
  // Posting list with delta encoding can only allow decreasing values.
  EXPECT_THAT(embedding_index_->CommitBufferToIndex(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(EmbeddingIndexTest, InvalidCommit_DocumentIdCanOnlyIncrease) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE));
  // Posting list with delta encoding can only allow decreasing values, which
  // means document ids must be committed increasingly, since document ids are
  // inverted in hit values.
  EXPECT_THAT(embedding_index_->CommitBufferToIndex(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(EmbeddingIndexTest, OptimizeShouldFailWithNullPointer) {
  EXPECT_THAT(embedding_index_->Optimize(
                  /*document_store=*/nullptr, schema_store_.get(),
                  /*document_id_old_to_new=*/{},
                  /*new_last_added_document_id=*/kInvalidDocumentId),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(embedding_index_->Optimize(
                  document_store_.get(), /*schema_store=*/nullptr,
                  /*document_id_old_to_new=*/{},
                  /*new_last_added_document_id=*/kInvalidDocumentId),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(EmbeddingIndexTest, OptimizeShouldFailWhenDocumentIdMapIsTooSmall) {
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), vector,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Optimize should fail because the provided document_id_old_to_new map does
  // not contain an entry for document id 2.
  EXPECT_THAT(embedding_index_
                  ->Optimize(document_store_.get(), schema_store_.get(),
                             /*document_id_old_to_new=*/{0, 1},
                             /*new_last_added_document_id=*/2)
                  .error_message(),
              HasSubstr("The provided map is too small"));
}

TEST_F(EmbeddingIndexTest, EmptyOptimizeIsOk) {
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{},
      /*new_last_added_document_id=*/kInvalidDocumentId));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(), Eq(0));
}

TEST_F(EmbeddingIndexTest, OptimizeSingleEmbeddingSingleDocument) {
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), vector,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Before optimize
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/2), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize without deleting any documents, and check that the index is
  // not changed.
  ICING_ASSERT_OK(
      embedding_index_->Optimize(document_store_.get(), schema_store_.get(),
                                 /*document_id_old_to_new=*/{0, 1, 2},
                                 /*new_last_added_document_id=*/2));
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/2), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize to map document id 2 to 1, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId, 1},
      /*new_last_added_document_id=*/1));
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/1), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete the document.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeSingleQuantizedEmbeddingSingleDocument) {
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2), vector,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Before optimize
  EmbeddingHit hit(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2),
                   /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     hit, /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize without deleting any documents, and check that the index is
  // not changed
  ICING_ASSERT_OK(
      embedding_index_->Optimize(document_store_.get(), schema_store_.get(),
                                 /*document_id_old_to_new=*/{0, 1, 2},
                                 /*new_last_added_document_id=*/2));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     hit, /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize to map document id 2 to 1, and check that the index is
  // updated correctly
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId, 1},
      /*new_last_added_document_id=*/1));
  hit = EmbeddingHit(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/1),
                     /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     hit, /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete the document
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(), Eq(0));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeMultipleEmbeddingsSingleDocument) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  PropertyProto::VectorProto vector3 = CreateVector("model", {0.4, 0.5, 0.6});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2), vector3,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Before optimize
  EmbeddingHit quantized_hit(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2),
      /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/3),
                  quantized_hit)));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector3.values())));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize without deleting any documents, and check that the index is
  // not changed.
  ICING_ASSERT_OK(
      embedding_index_->Optimize(document_store_.get(), schema_store_.get(),
                                 /*document_id_old_to_new=*/{0, 1, 2},
                                 /*new_last_added_document_id=*/2));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/3),
                  quantized_hit)));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector3.values())));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize to map document id 2 to 1, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId, 1},
      /*new_last_added_document_id=*/1));
  quantized_hit =
      EmbeddingHit(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/1),
                   /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/3),
                  quantized_hit)));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector3.values())));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete the document.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(), Eq(0));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeMultipleEmbeddingsMultipleDocument) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 = CreateVector("model", {1, 2, 3});
  PropertyProto::VectorProto vector3 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  PropertyProto::VectorProto vector4 = CreateVector("model", {0.4, 0.5, 0.6});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector3,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/1), vector4,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  // Before optimize
  EmbeddingHit quantized_hit(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/1),
      /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/6),
                  quantized_hit,
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                               /*location=*/3))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 0.3, 1, 2, 3, -0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector4.values())));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize without deleting any documents. It is expected to see that the
  // raw embedding data is rearranged, since during index transfer, embedding
  // vectors from higher document ids are added first.
  //
  // Also keep in mind that once the raw data is rearranged, calling another
  // Optimize subsequently will not change the raw data again.
  for (int i = 0; i < 2; i++) {
    ICING_ASSERT_OK(
        embedding_index_->Optimize(document_store_.get(), schema_store_.get(),
                                   /*document_id_old_to_new=*/{0, 1},
                                   /*new_last_added_document_id=*/1));
    EXPECT_THAT(
        GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                  /*model_signature=*/"model"),
        IsOkAndHolds(ElementsAre(
            EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                         /*location=*/0),
            quantized_hit,
            EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                         /*location=*/3),
            EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                         /*location=*/6))));
    EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
                ElementsAre(-0.1, -0.2, -0.3, 0.1, 0.2, 0.3, 1, 2, 3));
    EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
                Eq(3 + sizeof(Quantizer)));
    EXPECT_THAT(
        GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                       quantized_hit,
                                                       /*dimension=*/3),
        IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector4.values())));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 1);
  }

  // Run optimize to delete document 0, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{kInvalidDocumentId, 0},
      /*new_last_added_document_id=*/0));
  quantized_hit =
      EmbeddingHit(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                   /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0),
                  quantized_hit)));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(-0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector4.values())));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeEmbeddingsFromDifferentModels) {
  PropertyProto::VectorProto vector1 = CreateVector("model1", {0.1, 0.2});
  PropertyProto::VectorProto vector2 = CreateVector("model1", {1, 2});
  PropertyProto::VectorProto vector3 =
      CreateVector("model2", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/1), vector3,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  // Before optimize
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                        /*model_signature=*/"model1"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/2),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model2"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/1),
                               /*location=*/4))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, 1, 2, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize without deleting any documents. It is expected to see that the
  // raw embedding data is rearranged, since during index transfer:
  // - Embedding vectors with lower keys, which are the string encoded ordered
  //   pairs (dimension, model_signature), are iterated first.
  // - Embedding vectors from higher document ids are added first.
  //
  // Also keep in mind that once the raw data is rearranged, calling another
  // Optimize subsequently will not change the raw data again.
  for (int i = 0; i < 2; i++) {
    ICING_ASSERT_OK(
        embedding_index_->Optimize(document_store_.get(), schema_store_.get(),
                                   /*document_id_old_to_new=*/{0, 1},
                                   /*new_last_added_document_id=*/1));
    EXPECT_THAT(
        GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                  /*model_signature=*/"model1"),
        IsOkAndHolds(ElementsAre(
            EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                         /*location=*/0),
            EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                         /*location=*/2))));
    EXPECT_THAT(
        GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                  /*model_signature=*/"model2"),
        IsOkAndHolds(ElementsAre(
            EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/1),
                         /*location=*/4))));
    EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
                ElementsAre(1, 2, 0.1, 0.2, -0.1, -0.2, -0.3));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 1);
  }

  // Run optimize to delete document 1, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                        /*model_signature=*/"model1"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model2"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest,
       OptimizeEmbeddingsFromDifferentModelsAndDeleteTheFirst) {
  PropertyProto::VectorProto vector1 = CreateVector("model1", {0.1, 0.2});
  PropertyProto::VectorProto vector2 =
      CreateVector("model2", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/1), vector2,
      QUANTIZATION_TYPE_NONE));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  // Before optimize
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                        /*model_signature=*/"model1"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model2"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/1),
                               /*location=*/2))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.1, 0.2, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete document 0, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{kInvalidDocumentId, 0},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                        /*model_signature=*/"model1"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model2"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(-0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

}  // namespace
}  // namespace lib
}  // namespace icing
