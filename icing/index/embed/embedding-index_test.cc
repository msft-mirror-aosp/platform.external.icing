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
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-reference.h"
#include "icing/index/embed/quantizer.h"
#include "icing/index/hit/hit.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/gzip_stream.h"
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
#include "icing/util/document-util.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::FloatNear;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::Pointwise;
using ::testing::Test;

static constexpr SectionId kSectionIdQuantizedEmbedding = 2;
static constexpr float kEpsQuantized = 0.01f;

static constexpr uint32_t kDefaultDimension = 3;
static const char kDefaultModelSignature[] = "model";
static constexpr std::string_view kDefaultSchemaName = "type";

}  // namespace

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

    test_vector1_ = CreateVector(kDefaultModelSignature, {0.1, 0.2, 0.3});
    test_vector2_ = CreateVector(kDefaultModelSignature, {-0.1, -0.2, -0.3});
    test_vector3_ = CreateVector(kDefaultModelSignature, {0.4, 0.5, 0.6});

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &clock_, feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(
            &filesystem_, document_store_dir_, &clock_, schema_store_.get(),
            feature_flags_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/true,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionLevel,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionThresholdBytes,
            protobuf_ports::kDefaultMemLevel,
            /*initialize_stats=*/nullptr));
    document_store_ = std::move(create_result.document_store);

    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_, &clock_,
                               feature_flags_.get(),
                               /*num_shards=*/32));

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
    ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
        DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build())));
    ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
        DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build())));
    ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
        DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build())));

    default_shard_id_ = embedding_index_->GetShardId(
        kDefaultDimension, kDefaultModelSignature, kDefaultSchemaName);
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

  libtextclassifier3::StatusOr<uint32_t> AppendEmbeddingVector(
      const EmbeddingReference& embedding, uint32_t dimension,
      uint32_t shard_id) {
    return embedding_index_->AppendEmbeddingVector(embedding, dimension,
                                                   shard_id);
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

  PropertyProto::VectorProto test_vector1_;
  PropertyProto::VectorProto test_vector2_;
  PropertyProto::VectorProto test_vector3_;

  uint32_t default_shard_id_;
};

TEST_F(EmbeddingIndexTest, GetShardId) {
  // Hardcode some inputs to the GetShardId function, so that we can be aware of
  // any changes to the hashing function.
  EXPECT_EQ(embedding_index_->GetShardId(768, "model1", "schema1"), 10);
  EXPECT_EQ(embedding_index_->GetShardId(768, "model1", "schema2"), 4);
  EXPECT_EQ(embedding_index_->GetShardId(768, "model2", "schema1"), 20);
  EXPECT_EQ(embedding_index_->GetShardId(768, "model2", "schema2"), 14);
  EXPECT_EQ(embedding_index_->GetShardId(1024, "model1", "schema1"), 27);
  EXPECT_EQ(embedding_index_->GetShardId(1024, "model1", "schema2"), 21);
  EXPECT_EQ(embedding_index_->GetShardId(1024, "model2", "schema1"), 1);
  EXPECT_EQ(embedding_index_->GetShardId(1024, "model2", "schema2"), 27);
  EXPECT_EQ(embedding_index_->GetShardId(100, "aa", "bb"), 4);
  EXPECT_EQ(embedding_index_->GetShardId(100, "bb", "aa"), 20);
  EXPECT_EQ(embedding_index_->GetShardId(100, "aa", "aa"), 27);
  EXPECT_EQ(embedding_index_->GetShardId(100, "bb", "bb"), 29);
  EXPECT_EQ(embedding_index_->GetShardId(100, "aa", "aaa"), 18);
  EXPECT_EQ(embedding_index_->GetShardId(100, "bb", "bbb"), 11);
  EXPECT_EQ(embedding_index_->GetShardId(100, "aaa", "aa"), 4);
  EXPECT_EQ(embedding_index_->GetShardId(100, "bbb", "bb"), 13);
}

TEST_F(EmbeddingIndexTest, EmptyIndexContainsMetadataOnly) {
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
}

TEST_F(EmbeddingIndexTest, InitializationShouldFailWithNullPointer) {
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";

  EXPECT_THAT(EmbeddingIndex::Create(nullptr, embedding_index_dir, &clock_,
                                     feature_flags_.get(),
                                     /*num_shards=*/32),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(EmbeddingIndex::Create(&filesystem_, embedding_index_dir, nullptr,
                                     feature_flags_.get(),
                                     /*num_shards=*/32),
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
                             feature_flags_.get(),
                             /*num_shards=*/32));

  ICING_ASSERT_OK(embedding_index->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index->CommitBufferToIndex());
  embedding_index->set_last_added_document_id(0);

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index.get(), /*dimension=*/3,
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index->last_added_document_id(), 0);
  // GetChecksum should succeed without updating the checksum.
  ICING_EXPECT_OK(embedding_index->GetChecksum());

  // 2. Try to create another index with the same directory. This should fail
  // due to checksum mismatch.
  EXPECT_THAT(EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                                     feature_flags_.get(),
                                     /*num_shards=*/32),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  embedding_index.reset();
  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest, InitializationShouldFailWithZeroShards) {
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";
  EXPECT_THAT(EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                                     feature_flags_.get(),
                                     /*num_shards=*/0),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(EmbeddingIndexTest, InitializationShouldFailWithMismatchedNumShards) {
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";
  // 1. Create an index with num_shards = 1.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<EmbeddingIndex> embedding_index,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                               feature_flags_.get(), /*num_shards=*/1));
    ICING_ASSERT_OK(embedding_index->PersistToDisk());
  }

  // 2. Try to create another index with a different num_shards. This should
  // fail.
  EXPECT_THAT(EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                                     feature_flags_.get(),
                                     /*num_shards=*/32),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Mismatched number of shards")));

  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest,
       InitializationShouldSucceedWithNumShardsUpgradeFromZeroToOne) {
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";

  // 1. Create an index with num_shards = 1, and manually set num_shards to 0 in
  // the header.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<EmbeddingIndex> embedding_index,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                               feature_flags_.get(), /*num_shards=*/1));
    embedding_index->info().num_shards = 0;
    ICING_ASSERT_OK(embedding_index->PersistToDisk());
  }

  // 2. Re-initialize with num_shards = 1. It should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get(), /*num_shards=*/1));

  // 3. Check that num_shards in the header is now 1.
  EXPECT_EQ(embedding_index->num_shards(), 1);
  EXPECT_EQ(embedding_index->info().num_shards, 1);

  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest,
       InitializationShouldFailWithNumShardsUpgradeFromZeroToThirtyTwo) {
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";

  // 1. Create an index with num_shards = 1, and manually set num_shards to 0 in
  // the header.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<EmbeddingIndex> embedding_index,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                               feature_flags_.get(), /*num_shards=*/1));
    embedding_index->info().num_shards = 0;
    ICING_ASSERT_OK(embedding_index->PersistToDisk());
  }

  // 2. Re-initialize with num_shards = 32. It should fail.
  EXPECT_THAT(EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                                     feature_flags_.get(),
                                     /*num_shards=*/32),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Mismatched number of shards")));

  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest, InitializationShouldSucceedWithUpdateChecksums) {
  // 1. Create index and confirm that data was properly added.
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get(),
                             /*num_shards=*/32));

  ICING_ASSERT_OK(embedding_index->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index->CommitBufferToIndex());
  embedding_index->set_last_added_document_id(0);

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index.get(), /*dimension=*/3,
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index.get(), default_shard_id_),
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
                             feature_flags_.get(),
                             /*num_shards=*/32));

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_two.get(), /*dimension=*/3,
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_two.get(),
                                           default_shard_id_),
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
                             feature_flags_.get(),
                             /*num_shards=*/32));

  ICING_ASSERT_OK(embedding_index->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index->CommitBufferToIndex());
  embedding_index->set_last_added_document_id(0);

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index.get(), /*dimension=*/3,
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index->last_added_document_id(), 0);

  // 2. Update checksums to reflect the new content.
  ICING_EXPECT_OK(embedding_index->PersistToDisk());

  // 3. Create another index and confirm that the data is still there.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index_two,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get(),
                             /*num_shards=*/32));

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_two.get(), /*dimension=*/3,
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_two.get(),
                                           default_shard_id_),
              ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_two->last_added_document_id(), 0);

  embedding_index.reset();
  embedding_index_two.reset();
  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest, GetEmbeddingVectorShouldFailWhenOutOfRange) {
  BasicHit basic_hit(/*section_id=*/0, /*document_id=*/0);
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      basic_hit, test_vector1_, QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  EmbeddingHit embedding_hit(basic_hit, /*location=*/0);
  uint32_t dimension = 3;
  ICING_ASSERT_OK(embedding_index_->GetEmbeddingVector(embedding_hit, dimension,
                                                       default_shard_id_));
  EXPECT_THAT(embedding_index_->GetEmbeddingVector(embedding_hit, dimension + 1,
                                                   default_shard_id_),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
}

TEST_F(EmbeddingIndexTest,
       GetQuantizedEmbeddingVectorShouldFailWhenOutOfRange) {
  BasicHit basic_hit(kSectionIdQuantizedEmbedding, /*document_id=*/0);
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      basic_hit, test_vector1_, QUANTIZATION_TYPE_QUANTIZE_8_BIT,
      kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  EmbeddingHit embedding_hit(basic_hit, /*location=*/0);
  uint32_t dimension = 3;
  ICING_ASSERT_OK(embedding_index_->GetQuantizedEmbeddingVector(
      embedding_hit, dimension, default_shard_id_));
  EXPECT_THAT(embedding_index_->GetQuantizedEmbeddingVector(
                  embedding_hit, dimension + 1, default_shard_id_),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
}

TEST_F(EmbeddingIndexTest, AddSingleEmbedding) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, AppendEmbeddingReferenceDirectly) {
  // Buffer some embedding first.
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  // Test Float Vector
  std::vector<float> float_vector = {0.1f, 0.2f, 0.3f, 0.4f};
  EmbeddingReference float_ref;
  float_ref.float_vector = float_vector.data();
  ICING_ASSERT_OK_AND_ASSIGN(
      uint32_t float_location,
      AppendEmbeddingVector(float_ref, /*dimension=*/3, default_shard_id_));
  // Location should be after the first vector which had dimension=3
  EXPECT_EQ(float_location, 3);
  // Append it again with dimension=4
  ICING_ASSERT_OK_AND_ASSIGN(
      float_location,
      AppendEmbeddingVector(float_ref, /*dimension=*/4, default_shard_id_));
  EXPECT_EQ(float_location, 6);

  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, 0.1, 0.2, 0.3, 0.1, 0.2, 0.3, 0.4));

  // Test Quantized Vector
  ICING_ASSERT_OK_AND_ASSIGN(Quantizer quantizer,
                             Quantizer::Create(0.1f, 0.3f));
  std::vector<char> quantized_data(sizeof(Quantizer) + 4);
  memcpy(quantized_data.data(), &quantizer, sizeof(Quantizer));
  quantized_data[sizeof(Quantizer)] = quantizer.Quantize(0.1f);
  quantized_data[sizeof(Quantizer) + 1] = quantizer.Quantize(0.2f);
  quantized_data[sizeof(Quantizer) + 2] = quantizer.Quantize(0.3f);
  quantized_data[sizeof(Quantizer) + 3] = quantizer.Quantize(0.4f);

  EmbeddingReference quantized_ref;
  quantized_ref.quantized_vector = quantized_data.data();
  ICING_ASSERT_OK_AND_ASSIGN(
      uint32_t quantized_location,
      AppendEmbeddingVector(quantized_ref, /*dimension=*/3, default_shard_id_));
  EXPECT_EQ(quantized_location, 0);
  // Append it again with dimension=4
  ICING_ASSERT_OK_AND_ASSIGN(
      quantized_location,
      AppendEmbeddingVector(quantized_ref, /*dimension=*/4, default_shard_id_));
  EXPECT_EQ(quantized_location, 3 + sizeof(Quantizer));

  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + 4 + 2 * sizeof(Quantizer)));
}

TEST_F(EmbeddingIndexTest, AddSingleQuantizedEmbedding) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EmbeddingHit hit(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                   /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), hit,
          /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, AddMultipleEmbeddingsInTheSameSection) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/3))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, AddMultipleQuantizedEmbeddingsInTheSameSection) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0), test_vector2_,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EmbeddingHit hit1(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                    /*location=*/0);
  EmbeddingHit hit2(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                    /*location=*/3 + sizeof(Quantizer));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(hit1, hit2)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(2 * (3 + sizeof(Quantizer))));  // Two quantized vectors
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), hit1, /*dimension=*/3, kDefaultModelSignature,
          kDefaultSchemaName),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), hit2, /*dimension=*/3, kDefaultModelSignature,
          kDefaultSchemaName),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {-0.1, -0.2, -0.3})));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, HitsWithLowerSectionIdReturnedFirst) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/5, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/2, /*document_id=*/0), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/2, /*document_id=*/0),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/5, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, HitsWithHigherDocumentIdReturnedFirst) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);
}

TEST_F(EmbeddingIndexTest, AddEmbeddingsFromDifferentModels) {
  PropertyProto::VectorProto vector1 = CreateVector("model1", {0.1, 0.2});
  PropertyProto::VectorProto vector2 =
      CreateVector("model2", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
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
                               /*location=*/0))));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(
                  embedding_index_.get(),
                  /*dimension=*/5, /*model_signature=*/"non-existent-model"),
              IsOkAndHolds(IsEmpty()));
  // Check the shard for vector1.
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(
                  embedding_index_.get(),
                  embedding_index_->GetShardId(/*dimension=*/2,
                                               /*model_signature=*/"model1",
                                               kDefaultSchemaName)),
              ElementsAre(0.1, 0.2));
  // Check the shard for vector2.
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(
                  embedding_index_.get(),
                  embedding_index_->GetShardId(/*dimension=*/3,
                                               /*model_signature=*/"model2",
                                               kDefaultSchemaName)),
              ElementsAre(-0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest,
       AddEmbeddingsWithSameSignatureButDifferentDimension) {
  PropertyProto::VectorProto vector1 =
      CreateVector(kDefaultModelSignature, {0.1, 0.2});
  PropertyProto::VectorProto vector2 =
      CreateVector(kDefaultModelSignature, {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  // Check the shard for vector1.
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(
          embedding_index_.get(),
          embedding_index_->GetShardId(
              /*dimension=*/2,
              /*model_signature=*/kDefaultModelSignature, kDefaultSchemaName)),
      ElementsAre(0.1, 0.2));
  // Check the shard for vector2.
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(
          embedding_index_.get(),
          embedding_index_->GetShardId(
              /*dimension=*/3,
              /*model_signature=*/kDefaultModelSignature, kDefaultSchemaName)),
      ElementsAre(-0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, ClearIndex) {
  // Loop the same logic twice to make sure that clear works as expected, and
  // the index is still valid after clearing.
  for (int i = 0; i < 2; i++) {
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/1, /*document_id=*/0), test_vector1_,
        QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/2, /*document_id=*/1), test_vector2_,
        QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2),
        test_vector3_, QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
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
                                  kDefaultModelSignature),
        IsOkAndHolds(ElementsAre(hit1, hit2, hit3)));
    EXPECT_THAT(
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
        ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
    EXPECT_THAT(
        embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
        Eq(3 + sizeof(Quantizer)));
    EXPECT_THAT(
        GetAndRestoreQuantizedEmbeddingVectorFromIndex(
            embedding_index_.get(), hit1,
            /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
        IsOkAndHolds(
            Pointwise(FloatNear(kEpsQuantized), test_vector3_.values())));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 2);
    EXPECT_FALSE(embedding_index_->is_empty());
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(false));

    // Check that clear works as expected.
    ICING_ASSERT_OK(embedding_index_->Clear());
    EXPECT_TRUE(embedding_index_->is_empty());
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
    EXPECT_THAT(
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
        IsEmpty());
    EXPECT_THAT(
        embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
        Eq(0));
    EXPECT_EQ(embedding_index_->last_added_document_id(), kInvalidDocumentId);
  }
}

TEST_F(EmbeddingIndexTest, DiscardIndex) {
  // Loop the same logic twice to make sure that Discard works as expected, and
  // the index is still valid after discarding.
  for (int i = 0; i < 2; i++) {
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/1, /*document_id=*/0), test_vector1_,
        QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/2, /*document_id=*/1), test_vector2_,
        QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2),
        test_vector3_, QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
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
                                  kDefaultModelSignature),
        IsOkAndHolds(ElementsAre(hit1, hit2, hit3)));
    EXPECT_THAT(
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
        ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
    EXPECT_THAT(
        embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
        Eq(3 + sizeof(Quantizer)));
    EXPECT_THAT(
        GetAndRestoreQuantizedEmbeddingVectorFromIndex(
            embedding_index_.get(), hit1,
            /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
        IsOkAndHolds(
            Pointwise(FloatNear(kEpsQuantized), test_vector3_.values())));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 2);
    EXPECT_FALSE(embedding_index_->is_empty());
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(false));

    // Check that Discard works as expected.
    embedding_index_.reset();
    EmbeddingIndex::Discard(filesystem_, embedding_index_dir_);
    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_, &clock_,
                               feature_flags_.get(),
                               /*num_shards=*/32));
    EXPECT_TRUE(embedding_index_->is_empty());
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
    EXPECT_THAT(
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
        IsEmpty());
    EXPECT_THAT(
        embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
        Eq(0));
    EXPECT_EQ(embedding_index_->last_added_document_id(), kInvalidDocumentId);
  }
}

TEST_F(EmbeddingIndexTest, EmptyCommitIsOk) {
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(0));
}

TEST_F(EmbeddingIndexTest, MultipleCommits) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
}

TEST_F(EmbeddingIndexTest,
       InvalidCommit_SectionIdCanOnlyDecreaseForSingleDocument) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  // Posting list with delta encoding can only allow decreasing values.
  EXPECT_THAT(embedding_index_->CommitBufferToIndex(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(EmbeddingIndexTest, InvalidCommit_DocumentIdCanOnlyIncrease) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
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
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
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
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(0));
}

TEST_F(EmbeddingIndexTest, OptimizeSingleEmbeddingSingleDocument) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Before optimize
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/2), /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
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
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/2), /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
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
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/1), /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete the document.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeSingleQuantizedEmbeddingSingleDocument) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2), test_vector1_,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Before optimize
  EmbeddingHit hit(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2),
                   /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), hit, /*dimension=*/3, kDefaultModelSignature,
          kDefaultSchemaName),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize without deleting any documents, and check that the index is
  // not changed
  ICING_ASSERT_OK(
      embedding_index_->Optimize(document_store_.get(), schema_store_.get(),
                                 /*document_id_old_to_new=*/{0, 1, 2},
                                 /*new_last_added_document_id=*/2));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), hit, /*dimension=*/3, kDefaultModelSignature,
          kDefaultSchemaName),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
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
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), hit, /*dimension=*/3, kDefaultModelSignature,
          kDefaultSchemaName),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete the document
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(0));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeMultipleEmbeddingsSingleDocument) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2), test_vector3_,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Before optimize
  EmbeddingHit quantized_hit(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/2),
      /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/3),
                  quantized_hit)));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(GetAndRestoreQuantizedEmbeddingVectorFromIndex(
                  embedding_index_.get(), quantized_hit,
                  /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
              IsOkAndHolds(
                  Pointwise(FloatNear(kEpsQuantized), test_vector3_.values())));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize without deleting any documents, and check that the index is
  // not changed.
  ICING_ASSERT_OK(
      embedding_index_->Optimize(document_store_.get(), schema_store_.get(),
                                 /*document_id_old_to_new=*/{0, 1, 2},
                                 /*new_last_added_document_id=*/2));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/3),
                  quantized_hit)));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(GetAndRestoreQuantizedEmbeddingVectorFromIndex(
                  embedding_index_.get(), quantized_hit,
                  /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
              IsOkAndHolds(
                  Pointwise(FloatNear(kEpsQuantized), test_vector3_.values())));
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
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/3),
                  quantized_hit)));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(GetAndRestoreQuantizedEmbeddingVectorFromIndex(
                  embedding_index_.get(), quantized_hit,
                  /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
              IsOkAndHolds(
                  Pointwise(FloatNear(kEpsQuantized), test_vector3_.values())));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete the document.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(),
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(0));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeMultipleEmbeddingsMultipleDocument) {
  PropertyProto::VectorProto vector1 =
      CreateVector(kDefaultModelSignature, {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector(kDefaultModelSignature, {1, 2, 3});
  PropertyProto::VectorProto vector3 =
      CreateVector(kDefaultModelSignature, {-0.1, -0.2, -0.3});
  PropertyProto::VectorProto vector4 =
      CreateVector(kDefaultModelSignature, {0.4, 0.5, 0.6});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), vector2,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector3,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/1), vector4,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  // Before optimize
  EmbeddingHit quantized_hit(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/1),
      /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/6),
                  quantized_hit,
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                               /*location=*/3))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(0.1, 0.2, 0.3, 1, 2, 3, -0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), quantized_hit,
          /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
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
                                  kDefaultModelSignature),
        IsOkAndHolds(ElementsAre(
            EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                         /*location=*/0),
            quantized_hit,
            EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                         /*location=*/3),
            EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                         /*location=*/6))));
    EXPECT_THAT(
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
        ElementsAre(-0.1, -0.2, -0.3, 0.1, 0.2, 0.3, 1, 2, 3));
    EXPECT_THAT(
        embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
        Eq(3 + sizeof(Quantizer)));
    EXPECT_THAT(
        GetAndRestoreQuantizedEmbeddingVectorFromIndex(
            embedding_index_.get(), quantized_hit,
            /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
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
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0),
                  quantized_hit)));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      ElementsAre(-0.1, -0.2, -0.3));
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(default_shard_id_),
              Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), quantized_hit,
          /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), vector4.values())));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeEmbeddingsFromDifferentModels) {
  PropertyProto::VectorProto vector1 = CreateVector("model1", {0.1, 0.2});
  PropertyProto::VectorProto vector2 = CreateVector("model1", {1, 2});
  PropertyProto::VectorProto vector3 =
      CreateVector("model2", {-0.1, -0.2, -0.3});
  uint32_t vector1_and_vector2_shard_id = embedding_index_->GetShardId(
      /*dimension=*/2, "model1", kDefaultSchemaName);
  uint32_t vector3_shard_id = embedding_index_->GetShardId(
      /*dimension=*/3, "model2", kDefaultSchemaName);
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector2,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/1), vector3,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
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
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get(),
                                           vector1_and_vector2_shard_id),
              ElementsAre(0.1, 0.2, 1, 2));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), vector3_shard_id),
      ElementsAre(-0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize without deleting any documents. We should see the same data in
  // all the shards, since each shard only contains one vector.
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
                         /*location=*/0))));
    EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get(),
                                             vector1_and_vector2_shard_id),
                ElementsAre(1, 2, 0.1, 0.2));
    EXPECT_THAT(
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), vector3_shard_id),
        ElementsAre(-0.1, -0.2, -0.3));
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
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get(),
                                           vector1_and_vector2_shard_id),
              ElementsAre(0.1, 0.2));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), vector3_shard_id),
      IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest,
       OptimizeEmbeddingsFromDifferentModelsAndDeleteTheFirst) {
  PropertyProto::VectorProto vector1 = CreateVector("model1", {0.1, 0.2});
  PropertyProto::VectorProto vector2 =
      CreateVector("model2", {-0.1, -0.2, -0.3});
  uint32_t vector1_shard_id = embedding_index_->GetShardId(
      /*dimension=*/2, "model1", kDefaultSchemaName);
  uint32_t vector2_shard_id = embedding_index_->GetShardId(
      /*dimension=*/3, "model2", kDefaultSchemaName);
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/1), vector2,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
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
                               /*location=*/0))));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), vector1_shard_id),
      ElementsAre(0.1, 0.2));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), vector2_shard_id),
      ElementsAre(-0.1, -0.2, -0.3));
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
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), vector1_shard_id),
      IsEmpty());
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), vector2_shard_id),
      ElementsAre(-0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, ShardFileShouldBeLazilyCreated) {
  // Check no shard file exists after initialization.
  std::vector<std::string> files;
  ASSERT_TRUE(filesystem_.ListDirectory(embedding_index_dir_.c_str(),
                                        /*exclude=*/{},
                                        /*recursive=*/true, &files));
  for (const std::string& file : files) {
    EXPECT_THAT(file, Not(HasSubstr("embedding_vectors")));
  }

  // Add 5 embeddings, each with a different schema name, and should (possibly)
  // have a different shard id.
  std::unordered_set<uint32_t> shard_ids;
  for (int i = 0; i < 5; i++) {
    std::string schema_name = "schema" + std::to_string(i);
    uint32_t shard_id = embedding_index_->GetShardId(
        kDefaultDimension, kDefaultModelSignature, schema_name);
    shard_ids.insert(shard_id);

    // Add a non-quantized embedding, and check if its corresponding shard
    // file is created.
    std::string vector_file = absl_ports::StrCat(
        embedding_index_dir_, "/embedding_vectors_", std::to_string(shard_id));
    EXPECT_FALSE(filesystem_.FileExists(vector_file.c_str()));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
        QUANTIZATION_TYPE_NONE, schema_name));
    EXPECT_TRUE(filesystem_.FileExists(vector_file.c_str()));

    // Add a quantized embedding, and check if its corresponding shard file
    // is created.
    std::string quantized_vector_file = absl_ports::StrCat(
        embedding_index_dir_, "/quantized_embedding_vectors_",
        std::to_string(shard_id));
    EXPECT_FALSE(filesystem_.FileExists(quantized_vector_file.c_str()));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
        test_vector1_, QUANTIZATION_TYPE_QUANTIZE_8_BIT, schema_name));
    EXPECT_TRUE(filesystem_.FileExists(quantized_vector_file.c_str()));
  }
  EXPECT_EQ(shard_ids.size(), 5);
}

}  // namespace lib
}  // namespace icing
