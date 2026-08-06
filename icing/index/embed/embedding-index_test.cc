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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
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
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/embed/doc-hit-info-iterator-embedding-v2.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/index/embed/embedding-reference.h"
#include "icing/index/embed/quantizer.h"
#include "icing/index/hit/hit.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/gzip_stream.h"
#include "icing/proto/document.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/store/key-mapper.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/document-util.h"
#include "icing/util/embedding-util.h"
#include "icing/util/encode-util.h"

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

class EmbeddingIndexTest : public Test, public EmbeddingIndexTestPeer {
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

    default_shard_id_ = GetShardId(embedding_index_.get(), kDefaultDimension,
                                   kDefaultModelSignature, kDefaultSchemaName);
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    embedding_index_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  using IvfContextManager = ::icing::lib::EmbeddingIndex::IvfContextManager;
  using IvfMetadata = ::icing::lib::EmbeddingIndex::IvfMetadata;

  libtextclassifier3::StatusOr<IvfMetadata> GetMetadata(
      const IvfContextManager& ivf_context, EmbeddingIndex* index = nullptr) {
    if (index == nullptr) index = embedding_index_.get();
    return ivf_context.GetMetadata(index);
  }

  libtextclassifier3::Status SetMetadata(const IvfContextManager& ivf_context,
                                         const IvfMetadata& metadata,
                                         EmbeddingIndex* index = nullptr) {
    if (index == nullptr) index = embedding_index_.get();
    return ivf_context.SetMetadata(index, metadata);
  }

  libtextclassifier3::StatusOr<std::vector<uint32_t>>
  GetClosestClusterIdsByDistance(const IvfContextManager& ivf_context,
                                 const PropertyProto::VectorProto& query_vector,
                                 uint32_t num_clusters,
                                 EmbeddingIndex* index = nullptr) {
    if (index == nullptr) index = embedding_index_.get();
    return ivf_context.GetClosestClusterIdsByDistance(index, query_vector,
                                                      num_clusters);
  }

  bool PostingListExists(std::string_view key) {
    return GetPostingListMapper()->Get(key).ok();
  }

  static uint32_t GetNumShards(const EmbeddingIndex* index) {
    return index->info().num_shards;
  }

  static void SetNumShards(uint32_t num_shards, EmbeddingIndex* index) {
    index->info().num_shards = num_shards;
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

  KeyMapper<PostingListIdentifier>* GetPostingListMapper() {
    return embedding_index_->embedding_posting_list_mapper_.get();
  }

  // Returns a sorted list of unique base keys stored in the posting list
  // mapper. If `is_ivf` is true, it filters for keys that belong to an IVF
  // cluster. Otherwise, it filters for keys that belong to linear search.
  std::vector<std::string> GetKnownBaseKeys(bool is_ivf) {
    std::vector<std::string> keys;
    std::unique_ptr<KeyMapper<PostingListIdentifier>::Iterator> itr =
        GetPostingListMapper()->GetIterator();
    while (itr->Advance()) {
      std::string_view key = itr->GetKey();
      libtextclassifier3::StatusOr<embedding_util::ParsedPostingListKey>
          parsed_key_or = embedding_util::ParsePostingListKey(key);
      ICING_EXPECT_OK(parsed_key_or);
      embedding_util::ParsedPostingListKey parsed_key =
          std::move(parsed_key_or).ValueOrDie();
      bool parsed_key_is_ivf =
          parsed_key.cluster_id != embedding_util::kLinearSearchClusterId;
      if (parsed_key_is_ivf == is_ivf) {
        keys.push_back(std::string(parsed_key.base_key));
      }
    }
    std::sort(keys.begin(), keys.end());
    auto last = std::unique(keys.begin(), keys.end());
    keys.erase(last, keys.end());
    return keys;
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
  EXPECT_EQ(GetShardId(embedding_index_.get(), 768, "model1", "schema1"), 10);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 768, "model1", "schema2"), 4);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 768, "model2", "schema1"), 20);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 768, "model2", "schema2"), 14);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 1024, "model1", "schema1"), 27);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 1024, "model1", "schema2"), 21);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 1024, "model2", "schema1"), 1);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 1024, "model2", "schema2"), 27);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 100, "aa", "bb"), 4);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 100, "bb", "aa"), 20);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 100, "aa", "aa"), 27);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 100, "bb", "bb"), 29);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 100, "aa", "aaa"), 18);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 100, "bb", "bbb"), 11);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 100, "aaa", "aa"), 4);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 100, "bbb", "bb"), 13);

  // Tests with cluster_id.
  EXPECT_EQ(GetShardId(embedding_index_.get(), 768, "model1", "schema1",
                       /*cluster_id=*/0),
            10);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 768, "model1", "schema1",
                       /*cluster_id=*/1),
            8);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 768, "model1", "schema1",
                       /*cluster_id=*/10),
            4);
  EXPECT_EQ(GetShardId(embedding_index_.get(), 768, "model1", "schema1",
                       /*cluster_id=*/100),
            29);
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
    SetNumShards(0, embedding_index.get());
    ICING_ASSERT_OK(embedding_index->PersistToDisk());
  }

  // 2. Re-initialize with num_shards = 1. It should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex> embedding_index,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get(), /*num_shards=*/1));

  // 3. Check that num_shards in the header is now 1.
  EXPECT_EQ(embedding_index->num_shards(), 1);
  EXPECT_EQ(GetNumShards(embedding_index.get()), 1);

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
    SetNumShards(0, embedding_index.get());
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
  ICING_ASSERT_OK(GetEmbeddingVector(embedding_index_.get(), embedding_hit,
                                     dimension, default_shard_id_));
  EXPECT_THAT(GetEmbeddingVector(embedding_index_.get(), embedding_hit,
                                 dimension + 1, default_shard_id_),
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
  ICING_ASSERT_OK(GetQuantizedEmbeddingVector(
      embedding_index_.get(), embedding_hit, dimension, default_shard_id_));
  EXPECT_THAT(GetQuantizedEmbeddingVector(embedding_index_.get(), embedding_hit,
                                          dimension + 1, default_shard_id_),
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

  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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

TEST_F(EmbeddingIndexTest, AddSinglePreQuantizedEmbedding) {
  // Create pre-quantized data
  ICING_ASSERT_OK_AND_ASSIGN(Quantizer quantizer,
                             Quantizer::Create(0.1f, 0.3f));
  std::string quantized_values_str;
  quantized_values_str.resize(sizeof(Quantizer) + 3);
  memcpy(quantized_values_str.data(), &quantizer, sizeof(Quantizer));
  quantized_values_str[sizeof(Quantizer)] = quantizer.Quantize(0.1f);
  quantized_values_str[sizeof(Quantizer) + 1] = quantizer.Quantize(0.2f);
  quantized_values_str[sizeof(Quantizer) + 2] = quantizer.Quantize(0.3f);

  PropertyProto::VectorProto vector;
  vector.set_model_signature(kDefaultModelSignature);
  vector.set_quantized_values(quantized_values_str);

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0), vector,
      QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EmbeddingHit hit(BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                   /*location=*/0);
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        kDefaultModelSignature),
              IsOkAndHolds(ElementsAre(hit)));
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
      Eq(3 + sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(
          embedding_index_.get(), hit,
          /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
}

TEST_F(EmbeddingIndexTest,
       BufferEmbeddingShouldFailWithInvalidQuantizedValuesSize) {
  PropertyProto::VectorProto vector;
  vector.set_model_signature(kDefaultModelSignature);
  // Set quantized_values size to exactly sizeof(Quantizer)
  std::string quantized_values_str(sizeof(Quantizer), 'a');
  vector.set_quantized_values(quantized_values_str);

  EXPECT_THAT(embedding_index_->BufferEmbedding(
                  BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
                  vector, QUANTIZATION_TYPE_QUANTIZE_8_BIT, kDefaultSchemaName),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Quantized values size must be greater than "
                                 "sizeof(Quantizer")));
}

// Tests that BufferEmbedding return an error if quantization is not enabled
// but the vector has quantized values (fallback robustness check).
TEST_F(EmbeddingIndexTest,
       BufferEmbeddingShouldFailWithMismatchedQuantization) {
  PropertyProto::VectorProto vector;
  vector.set_model_signature(kDefaultModelSignature);
  // Valid length string
  std::string quantized_values_str(sizeof(Quantizer) + 4, 'a');
  vector.set_quantized_values(quantized_values_str);

  EXPECT_THAT(
      embedding_index_->BufferEmbedding(
          BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0), vector,
          QUANTIZATION_TYPE_NONE, kDefaultSchemaName),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
               HasSubstr("Property has 'quantized_values' set but schema "
                         "quantization_type is not QUANTIZE_8_BIT")));
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
                  GetShardId(embedding_index_.get(), /*dimension=*/2,
                             /*model_signature=*/"model1", kDefaultSchemaName)),
              ElementsAre(0.1, 0.2));
  // Check the shard for vector2.
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(
                  embedding_index_.get(),
                  GetShardId(embedding_index_.get(), /*dimension=*/3,
                             /*model_signature=*/"model2", kDefaultSchemaName)),
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
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(
                  embedding_index_.get(),
                  GetShardId(embedding_index_.get(),
                             /*dimension=*/2,
                             /*model_signature=*/kDefaultModelSignature,
                             kDefaultSchemaName)),
              ElementsAre(0.1, 0.2));
  // Check the shard for vector2.
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(
                  embedding_index_.get(),
                  GetShardId(embedding_index_.get(),
                             /*dimension=*/3,
                             /*model_signature=*/kDefaultModelSignature,
                             kDefaultSchemaName)),
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
        GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
        Eq(3 + sizeof(Quantizer)));
    EXPECT_THAT(
        GetAndRestoreQuantizedEmbeddingVectorFromIndex(
            embedding_index_.get(), hit1,
            /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
        IsOkAndHolds(
            Pointwise(FloatNear(kEpsQuantized), test_vector3_.values())));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 2);
    EXPECT_FALSE(is_empty(embedding_index_.get()));
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(false));

    // Check that clear works as expected.
    ICING_ASSERT_OK(embedding_index_->Clear());
    EXPECT_TRUE(is_empty(embedding_index_.get()));
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
    EXPECT_THAT(
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
        IsEmpty());
    EXPECT_THAT(
        GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
        GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
        Eq(3 + sizeof(Quantizer)));
    EXPECT_THAT(
        GetAndRestoreQuantizedEmbeddingVectorFromIndex(
            embedding_index_.get(), hit1,
            /*dimension=*/3, kDefaultModelSignature, kDefaultSchemaName),
        IsOkAndHolds(
            Pointwise(FloatNear(kEpsQuantized), test_vector3_.values())));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 2);
    EXPECT_FALSE(is_empty(embedding_index_.get()));
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(false));

    // Check that Discard works as expected.
    embedding_index_.reset();
    EmbeddingIndex::Discard(filesystem_, embedding_index_dir_);
    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_, &clock_,
                               feature_flags_.get(),
                               /*num_shards=*/32));
    EXPECT_TRUE(is_empty(embedding_index_.get()));
    EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
    EXPECT_THAT(
        GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
        IsEmpty());
    EXPECT_THAT(
        GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
        Eq(0));
    EXPECT_EQ(embedding_index_->last_added_document_id(), kInvalidDocumentId);
  }
}

TEST_F(EmbeddingIndexTest, EmptyCommitIsOk) {
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  EXPECT_TRUE(is_empty(embedding_index_.get()));
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_TRUE(is_empty(embedding_index_.get()));
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_TRUE(is_empty(embedding_index_.get()));
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_TRUE(is_empty(embedding_index_.get()));
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_TRUE(is_empty(embedding_index_.get()));
  EXPECT_THAT(IndexContainsMetadataOnly(), IsOkAndHolds(true));
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get(), default_shard_id_),
      IsEmpty());
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
        GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  EXPECT_THAT(
      GetTotalQuantizedVectorSize(embedding_index_.get(), default_shard_id_),
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
  uint32_t vector1_and_vector2_shard_id =
      GetShardId(embedding_index_.get(),
                 /*dimension=*/2, "model1", kDefaultSchemaName);
  uint32_t vector3_shard_id =
      GetShardId(embedding_index_.get(),
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
  uint32_t vector1_shard_id =
      GetShardId(embedding_index_.get(),
                 /*dimension=*/2, "model1", kDefaultSchemaName);
  uint32_t vector2_shard_id =
      GetShardId(embedding_index_.get(),
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
    uint32_t shard_id = GetShardId(embedding_index_.get(), kDefaultDimension,
                                   kDefaultModelSignature, schema_name);
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

TEST_F(EmbeddingIndexTest, IvfContextManager_Empty) {
  IvfContextManager ivf_context(kDefaultDimension, kDefaultModelSignature);
  // Get and Set metadata will be rejected for empty index.
  EXPECT_THAT(GetMetadata(ivf_context, embedding_index_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(SetMetadata(ivf_context, IvfMetadata(), embedding_index_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(GetClosestClusterIdsByDistance(ivf_context, test_vector1_,
                                             /*num_clusters=*/1,
                                             embedding_index_.get()),
              IsOkAndHolds(IsEmpty()));
}

TEST_F(EmbeddingIndexTest, IvfContextManager) {
  // Add an embedding to the index to make sure the index is not empty.
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));

  IvfContextManager ivf_context(kDefaultDimension, kDefaultModelSignature);

  // Initial metadata should be empty (all 0s)
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(metadata.num_clusters, 0);
  EXPECT_EQ(metadata.current_size, 0);
  EXPECT_EQ(metadata.last_ivf_build_size, 0);

  // Test GetClosestClusterIdsByDistance
  EXPECT_THAT(GetClosestClusterIdsByDistance(ivf_context, test_vector1_,
                                             /*num_clusters=*/1,
                                             embedding_index_.get()),
              IsOkAndHolds(IsEmpty()));

  // Set and get metadata
  metadata.num_clusters = 5;
  metadata.current_size = 10;
  metadata.last_ivf_build_size = 8;
  ICING_EXPECT_OK(SetMetadata(ivf_context, metadata, embedding_index_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata retrieved_metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(retrieved_metadata.num_clusters, 5);
  EXPECT_EQ(retrieved_metadata.current_size, 10);
  EXPECT_EQ(retrieved_metadata.last_ivf_build_size, 8);

  // Test GetPostingListKey
  EXPECT_EQ(ivf_context.GetPostingListKey(42),
            absl_ports::StrCat(ivf_context.base_key(),
                               embedding_util::kIvfPostingListKeySeparator,
                               encode_util::EncodeIntToCString(42)));
  EXPECT_EQ(IvfContextManager(ivf_context.base_key()).GetPostingListKey(42),
            absl_ports::StrCat(ivf_context.base_key(),
                               embedding_util::kIvfPostingListKeySeparator,
                               encode_util::EncodeIntToCString(42)));
}

TEST_F(EmbeddingIndexTest, MaintainAllIvf_EmptyIndexIsOk) {
  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(/*min_size_for_ivf=*/2);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));
}

TEST_F(EmbeddingIndexTest, MaintainAllIvf_AllEmbeddingsDeletedIsOk) {
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri_ivf").SetSchema("type").Build())));

  uint32_t min_size_for_ivf = 1;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/0,
                                          clock_.GetSystemTimeMilliseconds()));

  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_EXPECT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));
}

TEST_F(EmbeddingIndexTest, GetAccessor_MultipleClustersAreMergedCorrectly) {
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri3").SetSchema("type").Build())));

  uint32_t min_size_for_ivf = 2;
  // Buffer hits with specific document IDs
  // By maintaining IVF, these 4 vectors will be split into 2 clusters.
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector3_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/3), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(3);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // The actual clusters generated are 3 and 4.
  // Fetch hits out of both of them simultaneously to see if they are properly
  // merged.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor> accessor,
      embedding_index_->GetAccessor(kDefaultDimension, kDefaultModelSignature,
                                    {3, 4}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingIndex::EmbeddingHitAccessor::HitInfo> batch,
      accessor->GetNextHitsBatch());

  // Hits descend primarily by document_id since section_id is 0.
  ASSERT_EQ(batch.size(), 4);
  EXPECT_EQ(batch[0].hit.basic_hit().document_id(), 3);
  EXPECT_EQ(batch[1].hit.basic_hit().document_id(), 2);
  EXPECT_EQ(batch[2].hit.basic_hit().document_id(), 1);
  EXPECT_EQ(batch[3].hit.basic_hit().document_id(), 0);
}

TEST_F(EmbeddingIndexTest, GetAccessor_DuplicateClustersAreDeduplicated) {
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri_dup").SetSchema("type").Build())));

  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector3_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/3), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(3);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor> accessor,
      embedding_index_->GetAccessor(kDefaultDimension, kDefaultModelSignature,
                                    {3, 4, 3}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingIndex::EmbeddingHitAccessor::HitInfo> batch,
      accessor->GetNextHitsBatch());

  ASSERT_EQ(batch.size(), 4);
  EXPECT_EQ(batch[0].hit.basic_hit().document_id(), 3);
  EXPECT_EQ(batch[1].hit.basic_hit().document_id(), 2);
  EXPECT_EQ(batch[2].hit.basic_hit().document_id(), 1);
  EXPECT_EQ(batch[3].hit.basic_hit().document_id(), 0);
}

TEST_F(EmbeddingIndexTest, GetAccessor_MultipleClustersWithEmptyList) {
  // Add an embedding to make sure the index is not empty.
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  EXPECT_THAT(
      embedding_index_->GetAccessor(kDefaultDimension, kDefaultModelSignature,
                                    /*cluster_ids=*/{}),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(EmbeddingIndexTest,
       GetAccessor_MultipleClustersWithNonExistentClusters) {
  // Pass IVF cluster IDs that have not been built.
  EXPECT_THAT(
      embedding_index_->GetAccessor(kDefaultDimension, kDefaultModelSignature,
                                    /*cluster_ids=*/{100, 200}),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(EmbeddingIndexTest, GetAccessor_MultipleClustersWithSomeNonExistent) {
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri_ivf").SetSchema("type").Build())));

  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Clusters 3 and 4 exist. Request a mix of existing and non-existing
  // clusters.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor> accessor,
      embedding_index_->GetAccessor(kDefaultDimension, kDefaultModelSignature,
                                    {3, 100, 4}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingIndex::EmbeddingHitAccessor::HitInfo> batch,
      accessor->GetNextHitsBatch());

  // Both hits from the existing clusters should still be retrieved.
  ASSERT_EQ(batch.size(), 2);
  EXPECT_EQ(batch[0].hit.basic_hit().document_id(), 1);
  EXPECT_EQ(batch[1].hit.basic_hit().document_id(), 0);
}

TEST_F(EmbeddingIndexTest, GetAccessor_MultipleClustersWithBaseIndex) {
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri_ivf").SetSchema("type").Build())));

  // Add one hit to the base linear search index
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));

  // Add hits to the IVF index
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector3_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Fetch from the base linear index (kLinearSearchClusterId) and IVF clusters
  // (3 and 4).
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor> hit_accessor,
      embedding_index_->GetAccessor(
          kDefaultDimension, kDefaultModelSignature,
          {embedding_util::kLinearSearchClusterId, 3, 4}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingIndex::EmbeddingHitAccessor::HitInfo> batch,
      hit_accessor->GetNextHitsBatch());

  // All 3 hits should be retrieved and merged according to hit ordering.
  ASSERT_EQ(batch.size(), 3);
  EXPECT_EQ(batch[0].hit.basic_hit().document_id(), 2);
  EXPECT_EQ(batch[1].hit.basic_hit().document_id(), 1);
  EXPECT_EQ(batch[2].hit.basic_hit().document_id(), 0);
}

TEST_F(EmbeddingIndexTest, GetAccessorForVector_MultipleClusters) {
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri_ivf").SetSchema("type").Build())));

  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Test GetAccessorForVector with test_vector1_ requesting clusters 3 and 4.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor> accessor,
      embedding_index_->GetAccessorForVector(test_vector1_, {3, 4}));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingIndex::EmbeddingHitAccessor::HitInfo> batch,
      accessor->GetNextHitsBatch());

  ASSERT_EQ(batch.size(), 2);
  EXPECT_EQ(batch[0].hit.basic_hit().document_id(), 1);
  EXPECT_EQ(batch[1].hit.basic_hit().document_id(), 0);
}

TEST_F(EmbeddingIndexTest, IvfContextManager_GetClosestClusterIds_NotBuilt) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  IvfContextManager ivf_context(keys[0]);

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> clusters,
                             GetClosestClusterIdsByDistance(
                                 ivf_context, test_vector1_, /*num_clusters=*/1,
                                 embedding_index_.get()));
  EXPECT_THAT(clusters, IsEmpty());
}

TEST_F(EmbeddingIndexTest, IvfContextManager_GetClosestClusterIds) {
  // Add 4 embeddings to ensure we get more than 1 cluster.
  for (int i = 0; i < 4; ++i) {
    DocumentWrapper document_wrapper;
    *document_wrapper.mutable_document() =
        DocumentBuilder()
            .SetKey("namespace", absl_ports::StrCat("uri", std::to_string(i)))
            .SetSchema(std::string(kDefaultSchemaName))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                               document_store_->Put(document_wrapper));
    PropertyProto::VectorProto vector;
    vector.add_values(static_cast<float>(i));
    vector.add_values(static_cast<float>(i + 1));
    vector.add_values(static_cast<float>(i + 2));
    vector.set_model_signature(std::string(kDefaultModelSignature));
    ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
        BasicHit(/*section_id=*/0, put_result.new_document_id), vector,
        QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  }
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(/*min_size_for_ivf=*/2);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  IvfContextManager ivf_context(keys[0]);

  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  uint32_t num_clusters = metadata.num_clusters;
  ASSERT_EQ(num_clusters, 2);

  // Test k = 0
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> empty_clusters,
                             GetClosestClusterIdsByDistance(
                                 ivf_context, test_vector1_, /*num_clusters=*/0,
                                 embedding_index_.get()));
  EXPECT_THAT(empty_clusters, IsEmpty());

  // Test k = 1 (this previously caused a crash when k < num_clusters)
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> top_1_clusters,
                             GetClosestClusterIdsByDistance(
                                 ivf_context, test_vector1_, /*num_clusters=*/1,
                                 embedding_index_.get()));
  EXPECT_EQ(top_1_clusters.size(), 1);

  // Test k > num_clusters
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> all_clusters,
      GetClosestClusterIdsByDistance(ivf_context, test_vector1_,
                                     /*num_clusters=*/num_clusters + 5,
                                     embedding_index_.get()));
  EXPECT_EQ(all_clusters.size(), num_clusters);
}

TEST_F(EmbeddingIndexTest, BufferEmbeddingIvf_IntoDeltaStoreWhenNotBuilt) {
  // Buffer the IVF embedding. Since IVF isn't built, it goes to delta.
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  // Verify the posting list keys.
  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(metadata.current_size, 1);
  EXPECT_EQ(metadata.last_ivf_build_size, 0);
  EXPECT_EQ(metadata.num_clusters, 0);

  // Verify it went to the delta store.
  std::string delta_store_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId);
  EXPECT_TRUE(PostingListExists(delta_store_key));

  EmbeddingHit hit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                   /*location=*/0);
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), kDefaultDimension,
                                kDefaultModelSignature,
                                {embedding_util::kIvfDeltaStoreClusterId}),
      IsOkAndHolds(ElementsAre(hit)));
}

TEST_F(EmbeddingIndexTest, MaintainIvf_BuildsClustersCorrectly) {
  // Buffer enough IVF embeddings to trigger a cluster build.
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  // Trigger MaintainAllIvf to empty the delta store and build clusters.
  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Verify clustering occurred correctly.
  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(metadata.current_size, 2);
  EXPECT_EQ(metadata.last_ivf_build_size, 2);
  EXPECT_GT(metadata.num_clusters, 0);

  // The delta store posting list shouldn't even exist anymore.
  std::string delta_store_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId);
  EXPECT_FALSE(PostingListExists(delta_store_key));

  // Accessing it via the high-level API should return an empty set.
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), kDefaultDimension,
                                kDefaultModelSignature,
                                {embedding_util::kIvfDeltaStoreClusterId}),
      IsOkAndHolds(IsEmpty()));

  // The centroids posting list must exist and contain exactly num_clusters
  // hits.
  std::string centroids_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfCentroidsClusterId);
  EXPECT_TRUE(PostingListExists(centroids_key));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingHit> centroid_hits,
      GetEmbeddingHitsFromIndex(embedding_index_.get(), kDefaultDimension,
                                kDefaultModelSignature,
                                {embedding_util::kIvfCentroidsClusterId}));
  EXPECT_EQ(centroid_hits.size(), metadata.num_clusters);
}

TEST_F(EmbeddingIndexTest, MaintainIvf_BuildsMultipleClusters) {
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri3").SetSchema("type").Build())));

  // Buffer enough IVF embeddings to trigger a cluster build.
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector3_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/3), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(3);

  // Trigger MaintainAllIvf with target_cluster_size = 2 to build multiple
  // clusters.
  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Verify clustering occurred correctly.
  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(metadata.current_size, 4);
  EXPECT_EQ(metadata.last_ivf_build_size, 4);
  EXPECT_GT(metadata.num_clusters, 1);

  // The delta store posting list shouldn't even exist anymore.
  std::string delta_store_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId);
  EXPECT_FALSE(PostingListExists(delta_store_key));

  // The centroids posting list must exist and contain exactly num_clusters
  // hits.
  std::string centroids_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfCentroidsClusterId);
  EXPECT_TRUE(PostingListExists(centroids_key));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingHit> centroid_hits,
      GetEmbeddingHitsFromIndex(embedding_index_.get(), kDefaultDimension,
                                kDefaultModelSignature,
                                {embedding_util::kIvfCentroidsClusterId}));
  EXPECT_EQ(centroid_hits.size(), metadata.num_clusters);
}

TEST_F(EmbeddingIndexTest, BufferEmbeddingIvf_WithSameDocIdAndSectionId) {
  // Test adding multiple embeddings for the same doc and section.
  // This tests the `PrependHit` ordering invariant logic in
  // `CommitBufferToIndex` and `MaintainAllIvf`.
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector3_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));

  // Commit to index should prepend these 3 hits. They must be prepended
  // in strictly decreasing order.
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  // Trigger MaintainAllIvf with target_cluster_size = 2, so it distributes
  // the identical hits. It must also prepend them in strictly decreasing order.
  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // If we reach this point without crashing or returning INVALID_ARGUMENT,
  // the ordering logic is correct.
  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  // current_size should be 3.
  EXPECT_EQ(metadata.current_size, 3);
}

TEST_F(EmbeddingIndexTest, MaintainIvf_SortsAllHitsAcrossClusters) {
  // Test that MaintainAllIvf correctly sorts all_hits when retrieving hits
  // from multiple existing clusters. Without sorting, recombining these hits
  // into fewer clusters would cause PrependHit to fail its strictly-decreasing
  // invariant.
  uint32_t min_size_for_ivf = 2;
  constexpr int kNumEmbeddings = 100;
  std::mt19937 random(123);

  // Buffer 100 random embeddings, each with a unique DocumentId.
  for (int i = 0; i < kNumEmbeddings; ++i) {
    ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
        DocumentBuilder()
            .SetKey("namespace", std::to_string(i))
            .SetSchema("type")
            .Build())));
    ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
        BasicHit(/*section_id=*/0, /*document_id=*/i),
        GetRandomVector(random, "model", /*dimension=*/10),
        QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  }

  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(kNumEmbeddings - 1);

  // 1. Build initial clusters. We use target_cluster_size = 2 so it creates
  // 50 clusters. The embeddings will be scattered across these clusters,
  // likely out of document_id order.
  MaintainAnnIndexOptions maintain_options;
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(2);
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];
  IvfContextManager ivf_context(base_key);

  // Check metadata
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(metadata.last_ivf_build_size, 100);
  EXPECT_EQ(metadata.current_size, 100);
  EXPECT_EQ(metadata.num_clusters, 50);

  // 2. Trigger a rebuild into fewer clusters. By setting
  // target_cluster_size=100, all 100 embeddings will be placed into a single
  // new cluster. It reads from Cluster 0, 1, 2, 3... and concatenates the hits.
  // The concatenated list will NOT be sorted. If the code doesn't sort it,
  // prepending to the new cluster will fail with INVALID_ARGUMENT.
  maintain_options.mutable_mini_batch_k_means_options()
      ->set_target_cluster_size(100);
  maintain_options.set_rebuild_threshold(0.0);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Check metadata
  ICING_ASSERT_OK_AND_ASSIGN(metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(metadata.last_ivf_build_size, 100);
  EXPECT_EQ(metadata.current_size, 100);
  EXPECT_EQ(metadata.num_clusters, 1);
}

TEST_F(EmbeddingIndexTest, BufferEmbeddingIvf_IntoClustersWhenBuilt) {
  // Build initial clusters.
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Buffer another newly incoming embedding.
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector3_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // It should bypass the delta store and route straight to a
  // cluster.
  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  // current_size increases from 2 to 3, but last_ivf_build_size remains 2.
  EXPECT_EQ(metadata.current_size, 3);
  EXPECT_EQ(metadata.last_ivf_build_size, 2);

  std::string delta_store_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId);
  EXPECT_FALSE(PostingListExists(delta_store_key));

  // The delta store is still completely empty over the API check.
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), kDefaultDimension,
                                kDefaultModelSignature,
                                {embedding_util::kIvfDeltaStoreClusterId}),
      IsOkAndHolds(IsEmpty()));
}

TEST_F(EmbeddingIndexTest, MaintainIvf_RebuildsWhenThresholdMet) {
  // Setup initial clusters representing last_ivf_build_size = 2.
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Add 1 more element so current_size goes from 2 -> 3.
  // 3 >= 2 * (1.0 + 0.2), which crosses the 2.4 threshold exactly.
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector3_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Trigger MaintainAllIvf. It should rebuild.
  maintain_options.Clear();
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Verify the old clusters were reclaimed and new metadata matches.
  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  // Upon successful rebuild, last_ivf_build_size snaps to current_size.
  EXPECT_EQ(metadata.current_size, 3);
  EXPECT_EQ(metadata.last_ivf_build_size, 3);

  // Centroids should be properly instantiated.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingHit> centroid_hits,
      GetEmbeddingHitsFromIndex(embedding_index_.get(), kDefaultDimension,
                                kDefaultModelSignature,
                                {embedding_util::kIvfCentroidsClusterId}));
  EXPECT_EQ(centroid_hits.size(), metadata.num_clusters);
}

TEST_F(EmbeddingIndexTest, BufferEmbeddingIvf_And_LinearSearch_WorkTogether) {
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  size_t num_linear_keys = GetKnownBaseKeys(/*is_ivf=*/false).size();
  size_t num_ivf_keys = GetKnownBaseKeys(/*is_ivf=*/true).size();
  EXPECT_EQ(num_linear_keys, 1);
  EXPECT_EQ(num_ivf_keys, 1);

  EmbeddingHit linear_hit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                          /*location=*/0);
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), kDefaultDimension,
                                kDefaultModelSignature),
      IsOkAndHolds(ElementsAre(linear_hit)));

  EmbeddingHit ivf_hit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                       /*location=*/0);
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), kDefaultDimension,
                                kDefaultModelSignature,
                                {embedding_util::kIvfDeltaStoreClusterId}),
      IsOkAndHolds(ElementsAre(ivf_hit)));
}

TEST_F(EmbeddingIndexTest, MaintainIvf_BuildsClustersCorrectly_LargeDataset) {
  // Buffer 9000 unique embeddings to test dataset scale.
  int num_embeddings = 9000;
  for (int i = 0; i < num_embeddings; ++i) {
    DocumentWrapper document_wrapper;
    *document_wrapper.mutable_document() =
        DocumentBuilder()
            .SetKey("namespace", absl_ports::StrCat("uri", std::to_string(i)))
            .SetSchema(std::string(kDefaultSchemaName))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                               document_store_->Put(document_wrapper));
    DocumentId document_id = put_result.new_document_id;
    PropertyProto::VectorProto vector;
    vector.add_values(static_cast<float>(i));
    vector.add_values(static_cast<float>(i + 1));
    vector.set_model_signature(std::string(kDefaultModelSignature));
    ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
        BasicHit(/*section_id=*/0, document_id), vector, QUANTIZATION_TYPE_NONE,
        kDefaultSchemaName));
  }
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(num_embeddings - 1);

  MiniBatchKMeansOptions mini_batch_kmeans_options;
  int cluster_size = 100;
  mini_batch_kmeans_options.set_target_cluster_size(cluster_size);
  int expected_num_cluster = num_embeddings / cluster_size;
  // Trigger MaintainAllIvf to empty the delta store and build clusters.
  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(/*min_size_for_ivf=*/2);
  maintain_options.set_rebuild_threshold(0.2);
  *maintain_options.mutable_mini_batch_k_means_options() =
      mini_batch_kmeans_options;
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Verify clustering metrics handled large inputs successfully.
  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  // Verifying sizing and limits.
  EXPECT_EQ(metadata.current_size, num_embeddings);
  EXPECT_EQ(metadata.last_ivf_build_size, num_embeddings);
  EXPECT_EQ(metadata.num_clusters, expected_num_cluster);

  // The delta store posting list shouldn't exist.
  std::string delta_store_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId);
  EXPECT_FALSE(PostingListExists(delta_store_key));

  // The centroids posting list must exist and contain exactly num_clusters
  // hits.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingHit> centroid_hits,
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                kDefaultModelSignature,
                                {embedding_util::kIvfCentroidsClusterId}));
  EXPECT_EQ(centroid_hits.size(), expected_num_cluster);

  // Check the number of hits from all clusters.
  std::vector<uint32_t> cluster_ids;
  for (int i = 0; i < expected_num_cluster; ++i) {
    cluster_ids.push_back(i + embedding_util::kIvfBaseClusterId);
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingHit> all_hits,
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                kDefaultModelSignature, cluster_ids));
  EXPECT_EQ(all_hits.size(), num_embeddings);
}

TEST_F(EmbeddingIndexTest,
       MaintainIvf_BuildsClustersCorrectly_LargeDataset_OneShard) {
  // Create an embedding index with 1 shard to guarantee all writes go to
  // the same FileBackedVector.
  std::string embedding_index_dir =
      GetTestTempDir() + "/embedding_index_test_local";
  ICING_ASSERT_OK_AND_ASSIGN(
      embedding_index_,
      EmbeddingIndex::Create(&filesystem_, embedding_index_dir, &clock_,
                             feature_flags_.get(),
                             /*num_shards=*/1));

  int num_embeddings = 20000;
  for (int i = 0; i < num_embeddings; ++i) {
    DocumentWrapper document_wrapper;
    *document_wrapper.mutable_document() =
        DocumentBuilder()
            .SetKey("namespace", absl_ports::StrCat("uri", std::to_string(i)))
            .SetSchema(std::string(kDefaultSchemaName))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                               document_store_->Put(document_wrapper));
    DocumentId document_id = put_result.new_document_id;
    PropertyProto::VectorProto vector;
    vector.add_values(static_cast<float>(i));
    vector.add_values(static_cast<float>(i + 1));
    vector.set_model_signature(std::string(kDefaultModelSignature));
    ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
        BasicHit(/*section_id=*/0, document_id), vector, QUANTIZATION_TYPE_NONE,
        kDefaultSchemaName));
  }
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(num_embeddings - 1);

  MiniBatchKMeansOptions mini_batch_kmeans_options;
  int cluster_size = 200;
  mini_batch_kmeans_options.set_target_cluster_size(cluster_size);
  int expected_num_cluster = num_embeddings / cluster_size;

  // MaintainAllIvf executes K-Means and triggers Remaps!
  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(/*min_size_for_ivf=*/2);
  maintain_options.set_rebuild_threshold(0.2);
  *maintain_options.mutable_mini_batch_k_means_options() =
      mini_batch_kmeans_options;
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  // Verify clustering metrics handled large inputs successfully.
  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  // Verifying sizing and limits.
  EXPECT_EQ(metadata.current_size, num_embeddings);
  EXPECT_EQ(metadata.last_ivf_build_size, num_embeddings);
  EXPECT_EQ(metadata.num_clusters, expected_num_cluster);

  // The delta store posting list shouldn't exist.
  std::string delta_store_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId);
  EXPECT_FALSE(PostingListExists(delta_store_key));

  // The centroids posting list must exist and contain exactly num_clusters
  // hits.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingHit> centroid_hits,
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                kDefaultModelSignature,
                                {embedding_util::kIvfCentroidsClusterId}));
  EXPECT_EQ(centroid_hits.size(), expected_num_cluster);

  // Check the number of hits from all clusters.
  std::vector<uint32_t> cluster_ids;
  for (int i = 0; i < expected_num_cluster; ++i) {
    cluster_ids.push_back(i + embedding_util::kIvfBaseClusterId);
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingHit> all_hits,
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/2,
                                kDefaultModelSignature, cluster_ids));
  EXPECT_EQ(all_hits.size(), num_embeddings);

  filesystem_.DeleteDirectoryRecursively(embedding_index_dir.c_str());
}

TEST_F(EmbeddingIndexTest, BufferEmbeddingIvf_RebuildsAfterMultipleBatches) {
  // Define batching logic representing continuous ingestion.
  uint32_t min_size_for_ivf = 10;
  float rebuild_threshold = 0.5;  // Rebuild threshold set to 50%
  std::string model = kDefaultModelSignature;

  // Batch 1: Build initial configuration (15 elements).
  for (int i = 0; i < 15; ++i) {
    DocumentWrapper document_wrapper;
    *document_wrapper.mutable_document() =
        DocumentBuilder()
            .SetKey("namespace", absl_ports::StrCat("uri", std::to_string(i)))
            .SetSchema(std::string(kDefaultSchemaName))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                               document_store_->Put(document_wrapper));
    DocumentId document_id = put_result.new_document_id;
    PropertyProto::VectorProto vector;
    vector.add_values(static_cast<float>(i));
    vector.set_model_signature(kDefaultModelSignature);
    ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
        BasicHit(/*section_id=*/0, document_id), vector, QUANTIZATION_TYPE_NONE,
        kDefaultSchemaName));
  }
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(rebuild_threshold);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];
  IvfContextManager ivf_context(base_key);

  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata m1,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(m1.current_size, 15);
  EXPECT_EQ(m1.last_ivf_build_size, 15);

  // Batch 2: Add 7 elements. Current size becomes 22.
  // Threshold to cross: 15 * (1 + 0.5) = 22.5. So 22 does NOT trigger a
  // rebuild.
  for (int i = 15; i < 22; ++i) {
    DocumentWrapper document_wrapper;
    *document_wrapper.mutable_document() =
        DocumentBuilder()
            .SetKey("namespace", absl_ports::StrCat("uri", std::to_string(i)))
            .SetSchema(std::string(kDefaultSchemaName))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                               document_store_->Put(document_wrapper));
    DocumentId document_id = put_result.new_document_id;
    PropertyProto::VectorProto vector;
    vector.add_values(static_cast<float>(i));
    vector.set_model_signature(kDefaultModelSignature);
    ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
        BasicHit(/*section_id=*/0, document_id), vector, QUANTIZATION_TYPE_NONE,
        kDefaultSchemaName));
  }
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  MaintainAnnIndexOptions maintain_options2;
  maintain_options2.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options2.set_rebuild_threshold(rebuild_threshold);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options2));

  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata m2,
                             GetMetadata(ivf_context, embedding_index_.get()));
  // Size grew but build size remains static
  EXPECT_EQ(m2.current_size, 22);
  EXPECT_EQ(m2.last_ivf_build_size, 15);

  // Batch 3: Add 5 more elements. Current size becomes 27.
  // 27 > 22.5. This triggering threshold crosses exactly on act.
  for (int i = 22; i < 27; ++i) {
    DocumentWrapper document_wrapper;
    *document_wrapper.mutable_document() =
        DocumentBuilder()
            .SetKey("namespace", absl_ports::StrCat("uri", std::to_string(i)))
            .SetSchema(std::string(kDefaultSchemaName))
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                               document_store_->Put(document_wrapper));
    DocumentId document_id = put_result.new_document_id;
    PropertyProto::VectorProto vector;
    vector.add_values(static_cast<float>(i));
    vector.set_model_signature(kDefaultModelSignature);
    ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
        BasicHit(/*section_id=*/0, document_id), vector, QUANTIZATION_TYPE_NONE,
        kDefaultSchemaName));
  }
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  // Trigger MaintainAllIvf. It should rebuild exactly now.
  MaintainAnnIndexOptions maintain_options_rebuild;
  maintain_options_rebuild.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options_rebuild.set_rebuild_threshold(rebuild_threshold);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options_rebuild));

  // Validate successful consecutive cluster rebuilding.
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata m3,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(m3.current_size, 27);
  // Rebuild occurred. last_ivf_build_size snapped forward.
  EXPECT_EQ(m3.last_ivf_build_size, 27);

  // Centroids count accurately preserved after multiple iterations of
  // destruction and rebuilding
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<EmbeddingHit> centroid_hits,
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/1, model,
                                {embedding_util::kIvfCentroidsClusterId}));
  EXPECT_EQ(centroid_hits.size(), m3.num_clusters);
}

TEST_F(EmbeddingIndexTest, TransferIndex_WithIvfMetadata) {
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  std::vector<DocumentId> document_id_old_to_new(2);
  document_id_old_to_new[0] = 0;
  document_id_old_to_new[1] = 1;

  ICING_ASSERT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(), document_id_old_to_new, 1));

  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(metadata.current_size, 2);
  EXPECT_EQ(metadata.last_ivf_build_size, 2);
  EXPECT_GT(metadata.num_clusters, 0);

  std::string centroids_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfCentroidsClusterId);
  EXPECT_TRUE(PostingListExists(centroids_key));
}

TEST_F(EmbeddingIndexTest, MaintainIvf_IgnoresNotFoundDuringDeletion) {
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/1), test_vector2_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  // MaintainAllIvf builds clusters and drops the delta store.
  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  std::vector<std::string> keys = GetKnownBaseKeys(/*is_ivf=*/true);
  ASSERT_EQ(keys.size(), 1);
  std::string base_key = keys[0];

  IvfContextManager ivf_context(base_key);
  std::string delta_store_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId);
  EXPECT_FALSE(PostingListExists(delta_store_key));

  // Add another cluster directly to surpass the rebuild threshold so that it
  // forces a rebuild. 3 >= 2 * (1.0 + 0.2), which crosses the 2.4 threshold.
  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/2), test_vector3_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // This second maintain call will attempt to delete the delta store, which
  // currently is empty and doesn't exist. We expect this to succeed without
  // returning NOT_FOUND.
  maintain_options.Clear();
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_EXPECT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  ICING_ASSERT_OK_AND_ASSIGN(IvfMetadata metadata,
                             GetMetadata(ivf_context, embedding_index_.get()));
  EXPECT_EQ(metadata.current_size, 3);
  EXPECT_EQ(metadata.last_ivf_build_size, 3);
}

TEST_F(EmbeddingIndexTest, OptimizeWithMetadataToEmptyIndex) {
  uint32_t min_size_for_ivf = 2;
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder()
          .SetKey("ns", "uri1")
          .SetSchema(std::string(kDefaultSchemaName))
          .Build())));

  ICING_ASSERT_OK(embedding_index_->BufferEmbeddingIvf(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  MaintainAnnIndexOptions maintain_options;
  maintain_options.set_min_size_for_ivf(min_size_for_ivf);
  maintain_options.set_rebuild_threshold(0.2);
  ICING_ASSERT_OK(embedding_index_->MaintainAllIvf(
      *document_store_, *schema_store_, maintain_options));

  std::vector<DocumentId> document_id_old_to_new(1);
  document_id_old_to_new[0] = kInvalidDocumentId;

  // Optimize mapping all documents to kInvalidDocumentId will result in the new
  // index being completely empty.
  ICING_EXPECT_OK(embedding_index_->Optimize(
      document_store_.get(), schema_store_.get(), document_id_old_to_new, 0));
}

TEST_F(EmbeddingIndexTest, CanAdoptDelegateReturnsFalseWhenHasDelegate) {
  PropertyProto::VectorProto query_vector = test_vector1_;

  EmbeddingQueryResults::EmbeddingQueryMatchInfoMap info_map;
  std::vector<double> global_scores;
  std::vector<uint32_t> cluster_ids = {embedding_util::kLinearSearchClusterId};

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIteratorEmbeddingV2> iterator,
      DocHitInfoIteratorEmbeddingV2::Create(
          &query_vector, SearchSpecProto::EmbeddingQueryMetricType::COSINE,
          /*score_low=*/0.0, /*score_high=*/1.0, &info_map, &global_scores,
          /*global_section_infos=*/nullptr, cluster_ids, embedding_index_.get(),
          document_store_.get(), schema_store_.get(), /*current_time_ms=*/0));

  EXPECT_TRUE(iterator->CanAdoptDelegate());

  auto dummy_delegate = std::make_unique<DocHitInfoIteratorDummy>();
  iterator->AdoptDelegate(std::move(dummy_delegate),
                          /*delegate_node_is_right_most=*/true);

  EXPECT_FALSE(iterator->CanAdoptDelegate());
}

TEST_F(EmbeddingIndexTest, ResetsDocHitInfoWhenEncounteringError) {
  PropertyProto::VectorProto query_vector = test_vector1_;
  DocumentId document_id = 0;

  EmbeddingQueryResults embedding_query_results(/*num_query_vectors=*/1);
  std::vector<double> global_scores = {0.1};

  // Add a single document to the index.
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), test_vector1_,
      QUANTIZATION_TYPE_NONE, kDefaultSchemaName));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  // Add a single score to the embedding query results.
  ICING_ASSERT_OK_AND_ASSIGN(
      EmbeddingQueryResults::EmbeddingQueryMatchInfoMap * info_map,
      embedding_query_results.GetOrCreateMatchInfoMap(
          /*query_vector_index=*/0,
          SearchSpecProto::EmbeddingQueryMetricType::COSINE));
  (*info_map)[document_id].AppendScore(global_scores, 0.1);

  std::vector<uint32_t> cluster_ids = {embedding_util::kLinearSearchClusterId};

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocHitInfoIteratorEmbeddingV2> iterator,
      DocHitInfoIteratorEmbeddingV2::Create(
          &query_vector, SearchSpecProto::EmbeddingQueryMetricType::COSINE,
          /*score_low=*/0.0, /*score_high=*/1.0, info_map, &global_scores,
          /*global_section_infos=*/nullptr, cluster_ids, embedding_index_.get(),
          document_store_.get(), schema_store_.get(), /*current_time_ms=*/0));

  // Advance to the only document.
  iterator->Advance();
  EXPECT_EQ(iterator->doc_hit_info().document_id(), document_id);

  // Advance again to trigger an error.
  iterator->Advance();
  // Verify that the document id is reset to kInvalidDocumentId and not the last
  // valid document id.
  EXPECT_EQ(iterator->doc_hit_info().document_id(), kInvalidDocumentId);
}

}  // namespace lib
}  // namespace icing
