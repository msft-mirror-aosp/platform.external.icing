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

#include "icing/index/embedding-indexing-handler.h"

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/embed/quantizer.h"
#include "icing/index/hit/hit.h"
#include "icing/portable/platform.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-data.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/util/icu-data-file-helper.h"
#include "icing/util/tokenized-document.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::FloatNear;
using ::testing::IsEmpty;
using ::testing::IsTrue;
using ::testing::Pointwise;

// Indexable properties (section) and section id. Section id is determined by
// the lexicographical order of indexable property paths.
// Schema type with indexable properties: FakeType
// Section id = 0: "body"
// Section id = 1: "bodyEmbedding"
// Section id = 2: "quantizedEmbedding"
// Section id = 3: "title"
// Section id = 4: "titleEmbedding"
static constexpr std::string_view kFakeType = "FakeType";
static constexpr std::string_view kPropertyBody = "body";
static constexpr std::string_view kPropertyBodyEmbedding = "bodyEmbedding";
static constexpr std::string_view kPropertyQuantizedEmbedding =
    "quantizedEmbedding";
static constexpr std::string_view kPropertyTitle = "title";
static constexpr std::string_view kPropertyTitleEmbedding = "titleEmbedding";
static constexpr std::string_view kPropertyNonIndexableEmbedding =
    "nonIndexableEmbedding";

static constexpr SectionId kSectionIdBodyEmbedding = 1;
static constexpr SectionId kSectionIdQuantizedEmbedding = 2;
static constexpr SectionId kSectionIdTitleEmbedding = 4;

// Schema type with nested indexable properties: FakeCollectionType
// Section id = 0: "collection.body"
// Section id = 1: "collection.bodyEmbedding"
// Section id = 2: "collection.quantizedEmbedding"
// Section id = 3: "collection.title"
// Section id = 4: "collection.titleEmbedding"
// Section id = 5: "fullDocEmbedding"
static constexpr std::string_view kFakeCollectionType = "FakeCollectionType";
static constexpr std::string_view kPropertyCollection = "collection";
static constexpr std::string_view kPropertyFullDocEmbedding =
    "fullDocEmbedding";

static constexpr SectionId kSectionIdNestedBodyEmbedding = 1;
static constexpr SectionId kSectionIdNestedQuantizedEmbedding = 2;
static constexpr SectionId kSectionIdNestedTitleEmbedding = 4;
static constexpr SectionId kSectionIdFullDocEmbedding = 5;

constexpr float kEpsQuantized = 0.01f;

class EmbeddingIndexingHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpIcuDataFile(
              GetTestFilePath("icing/icu.dat")));
    }

    base_dir_ = GetTestTempDir() + "/icing_test";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(base_dir_.c_str()),
                IsTrue());

    embedding_index_working_path_ = base_dir_ + "/embedding_index";
    schema_store_dir_ = base_dir_ + "/schema_store";
    document_store_dir_ = base_dir_ + "/document_store";

    language_segmenter_factory::SegmenterOptions segmenter_options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        lang_segmenter_,
        language_segmenter_factory::Create(std::move(segmenter_options)));

    ASSERT_THAT(
        filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str()),
        IsTrue());
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &fake_clock_, feature_flags_.get()));
    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType(kFakeType)
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kPropertyTitle)
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kPropertyBody)
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName(kPropertyTitleEmbedding)
                            .SetDataTypeVector(
                                EmbeddingIndexingConfig::EmbeddingIndexingType::
                                    LINEAR_SEARCH)
                            .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName(kPropertyBodyEmbedding)
                            .SetDataTypeVector(
                                EmbeddingIndexingConfig::EmbeddingIndexingType::
                                    LINEAR_SEARCH)
                            .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName(kPropertyQuantizedEmbedding)
                            .SetDataTypeVector(
                                EmbeddingIndexingConfig::EmbeddingIndexingType::
                                    LINEAR_SEARCH,
                                QUANTIZATION_TYPE_QUANTIZE_8_BIT)
                            .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName(kPropertyNonIndexableEmbedding)
                                     .SetDataType(TYPE_VECTOR)
                                     .SetCardinality(CARDINALITY_REPEATED)))
            .AddType(SchemaTypeConfigBuilder()
                         .SetType(kFakeCollectionType)
                         .AddProperty(PropertyConfigBuilder()
                                          .SetName(kPropertyCollection)
                                          .SetDataTypeDocument(
                                              kFakeType,
                                              /*index_nested_properties=*/true)
                                          .SetCardinality(CARDINALITY_REPEATED))
                         .AddProperty(
                             PropertyConfigBuilder()
                                 .SetName(kPropertyFullDocEmbedding)
                                 .SetDataTypeVector(
                                     EmbeddingIndexingConfig::
                                         EmbeddingIndexingType::LINEAR_SEARCH)
                                 .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    ASSERT_TRUE(
        filesystem_.CreateDirectoryRecursively(document_store_dir_.c_str()));
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult doc_store_create_result,
        DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                              schema_store_.get(), feature_flags_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*pre_mapping_fbv=*/false,
                              /*use_persistent_hash_map=*/true,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDefaultCompressionLevel,
                              /*initialize_stats=*/nullptr));
    document_store_ = std::move(doc_store_create_result.document_store);

    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_working_path_,
                               &fake_clock_, feature_flags_.get()));
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    lang_segmenter_.reset();
    embedding_index_.reset();

    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  FakeClock fake_clock_;
  std::string base_dir_;
  std::string embedding_index_working_path_;
  std::string schema_store_dir_;
  std::string document_store_dir_;

  std::unique_ptr<EmbeddingIndex> embedding_index_;
  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
};

}  // namespace

TEST_F(EmbeddingIndexingHandlerTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(EmbeddingIndexingHandler::Create(/*clock=*/nullptr,
                                               embedding_index_.get(),
                                               /*enable_embedding_index=*/true),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(EmbeddingIndexingHandler::Create(&fake_clock_,
                                               /*embedding_index=*/nullptr,
                                               /*enable_embedding_index=*/true),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(EmbeddingIndexingHandlerTest, HandleEmbeddingSection) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kPropertyTitle), "title")
          .AddVectorProperty(std::string(kPropertyTitleEmbedding),
                             CreateVector("model", {0.1, 0.2, 0.3}))
          .AddStringProperty(std::string(kPropertyBody), "body")
          .AddVectorProperty(std::string(kPropertyBodyEmbedding),
                             CreateVector("model", {0.4, 0.5, 0.6}),
                             CreateVector("model", {0.7, 0.8, 0.9}))
          .AddVectorProperty(std::string(kPropertyQuantizedEmbedding),
                             CreateVector("model", {0.1, 0.2, 0.3}),
                             CreateVector("model", {0.4, 0.5, 0.6}))
          .AddVectorProperty(std::string(kPropertyNonIndexableEmbedding),
                             CreateVector("model", {1.1, 1.2, 1.3}))
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                std::move(document)));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result,
      document_store_->Put(tokenized_document.document()));
  DocumentId document_id = put_result.new_document_id;

  ASSERT_THAT(embedding_index_->last_added_document_id(),
              Eq(kInvalidDocumentId));
  // Handle document.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndexingHandler> handler,
      EmbeddingIndexingHandler::Create(&fake_clock_, embedding_index_.get(),
                                       /*enable_embedding_index=*/true));
  EXPECT_THAT(handler->Handle(
                  tokenized_document, document_id, put_result.old_document_id,
                  /*recovery_mode=*/false, /*put_document_stats=*/nullptr),
              IsOk());

  // Check index
  EmbeddingHit hit1(BasicHit(kSectionIdBodyEmbedding, /*document_id=*/0),
                    /*location=*/0);
  EmbeddingHit hit2(BasicHit(kSectionIdBodyEmbedding, /*document_id=*/0),
                    /*location=*/3);
  EmbeddingHit hit3(BasicHit(kSectionIdTitleEmbedding, /*document_id=*/0),
                    /*location=*/6);
  // Quantized embeddings are stored in a different location from unquantized
  // embeddings, so the location starts from 0 again.
  EmbeddingHit quantized_hit1(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
      /*location=*/0);
  EmbeddingHit quantized_hit2(
      BasicHit(kSectionIdQuantizedEmbedding, /*document_id=*/0),
      /*location=*/3 + sizeof(Quantizer));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(hit1, hit2, quantized_hit1,
                                       quantized_hit2, hit3)));
  // Check unquantized embedding data
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2, 0.3));
  // Check quantized embedding data
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(6 + 2 * sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit1,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit2,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.4, 0.5, 0.6})));

  EXPECT_THAT(embedding_index_->last_added_document_id(), Eq(document_id));
}

TEST_F(EmbeddingIndexingHandlerTest, EmbeddingShouldNotBeIndexedIfDisabled) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kPropertyTitle), "title")
          .AddVectorProperty(std::string(kPropertyTitleEmbedding),
                             CreateVector("model", {0.1, 0.2, 0.3}))
          .AddStringProperty(std::string(kPropertyBody), "body")
          .AddVectorProperty(std::string(kPropertyBodyEmbedding),
                             CreateVector("model", {0.4, 0.5, 0.6}),
                             CreateVector("model", {0.7, 0.8, 0.9}))
          .AddVectorProperty(std::string(kPropertyNonIndexableEmbedding),
                             CreateVector("model", {1.1, 1.2, 1.3}))
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                std::move(document)));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result,
      document_store_->Put(tokenized_document.document()));
  DocumentId document_id = put_result.new_document_id;

  ASSERT_THAT(embedding_index_->last_added_document_id(),
              Eq(kInvalidDocumentId));
  // If enable_embedding_index is false, the handler should not index any
  // embeddings.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndexingHandler> handler,
      EmbeddingIndexingHandler::Create(&fake_clock_, embedding_index_.get(),
                                       /*enable_embedding_index=*/false));
  EXPECT_THAT(handler->Handle(
                  tokenized_document, document_id, put_result.old_document_id,
                  /*recovery_mode=*/false, /*put_document_stats=*/nullptr),
              IsOk());

  // Check that the embedding index is empty.
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
  EXPECT_THAT(embedding_index_->last_added_document_id(), Eq(document_id));
}

TEST_F(EmbeddingIndexingHandlerTest, HandleNestedEmbeddingSection) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_collection_type/1")
          .SetSchema(std::string(kFakeCollectionType))
          .AddDocumentProperty(
              std::string(kPropertyCollection),
              DocumentBuilder()
                  .SetKey("icing", "nested_fake_type/1")
                  .SetSchema(std::string(kFakeType))
                  .AddStringProperty(std::string(kPropertyTitle), "title")
                  .AddVectorProperty(std::string(kPropertyTitleEmbedding),
                                     CreateVector("model", {0.1, 0.2, 0.3}))
                  .AddStringProperty(std::string(kPropertyBody), "body")
                  .AddVectorProperty(std::string(kPropertyBodyEmbedding),
                                     CreateVector("model", {0.4, 0.5, 0.6}),
                                     CreateVector("model", {0.7, 0.8, 0.9}))
                  .AddVectorProperty(std::string(kPropertyQuantizedEmbedding),
                                     CreateVector("model", {0.1, 0.2, 0.3}),
                                     CreateVector("model", {0.4, 0.5, 0.6}))
                  .AddVectorProperty(
                      std::string(kPropertyNonIndexableEmbedding),
                      CreateVector("model", {1.1, 1.2, 1.3}))
                  .Build())
          .AddVectorProperty(std::string(kPropertyFullDocEmbedding),
                             CreateVector("model", {2.1, 2.2, 2.3}))
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                std::move(document)));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result,
      document_store_->Put(tokenized_document.document()));
  DocumentId document_id = put_result.new_document_id;

  ASSERT_THAT(embedding_index_->last_added_document_id(),
              Eq(kInvalidDocumentId));
  // Handle document.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndexingHandler> handler,
      EmbeddingIndexingHandler::Create(&fake_clock_, embedding_index_.get(),
                                       /*enable_embedding_index=*/true));
  EXPECT_THAT(handler->Handle(
                  tokenized_document, document_id, put_result.old_document_id,
                  /*recovery_mode=*/false, /*put_document_stats=*/nullptr),
              IsOk());

  // Check index
  EmbeddingHit hit1(BasicHit(kSectionIdNestedBodyEmbedding, /*document_id=*/0),
                    /*location=*/0);
  EmbeddingHit hit2(BasicHit(kSectionIdNestedBodyEmbedding, /*document_id=*/0),
                    /*location=*/3);
  EmbeddingHit hit3(BasicHit(kSectionIdNestedTitleEmbedding, /*document_id=*/0),
                    /*location=*/6);
  EmbeddingHit hit4(BasicHit(kSectionIdFullDocEmbedding, /*document_id=*/0),
                    /*location=*/9);
  // Quantized embeddings are stored in a different location from unquantized
  // embeddings, so the location starts from 0 again.
  EmbeddingHit quantized_hit1(
      BasicHit(kSectionIdNestedQuantizedEmbedding, /*document_id=*/0),
      /*location=*/0);
  EmbeddingHit quantized_hit2(
      BasicHit(kSectionIdNestedQuantizedEmbedding, /*document_id=*/0),
      /*location=*/3 + sizeof(Quantizer));
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(hit1, hit2, quantized_hit1,
                                       quantized_hit2, hit3, hit4)));
  // Check unquantized embedding data
  EXPECT_THAT(
      GetRawEmbeddingDataFromIndex(embedding_index_.get()),
      ElementsAre(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2, 0.3, 2.1, 2.2, 2.3));
  // Check quantized embedding data
  EXPECT_THAT(embedding_index_->GetTotalQuantizedVectorSize(),
              Eq(6 + 2 * sizeof(Quantizer)));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit1,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.1, 0.2, 0.3})));
  EXPECT_THAT(
      GetAndRestoreQuantizedEmbeddingVectorFromIndex(embedding_index_.get(),
                                                     quantized_hit2,
                                                     /*dimension=*/3),
      IsOkAndHolds(Pointwise(FloatNear(kEpsQuantized), {0.4, 0.5, 0.6})));

  EXPECT_THAT(embedding_index_->last_added_document_id(), Eq(document_id));
}

TEST_F(EmbeddingIndexingHandlerTest,
       HandleInvalidNewDocumentIdShouldReturnInvalidArgumentError) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kPropertyTitle), "title")
          .AddVectorProperty(std::string(kPropertyTitleEmbedding),
                             CreateVector("model", {0.1, 0.2, 0.3}))
          .AddStringProperty(std::string(kPropertyBody), "body")
          .AddVectorProperty(std::string(kPropertyBodyEmbedding),
                             CreateVector("model", {0.4, 0.5, 0.6}),
                             CreateVector("model", {0.7, 0.8, 0.9}))
          .AddVectorProperty(std::string(kPropertyNonIndexableEmbedding),
                             CreateVector("model", {1.1, 1.2, 1.3}))
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                std::move(document)));
  ICING_ASSERT_OK(document_store_->Put(tokenized_document.document()));

  static constexpr DocumentId kCurrentDocumentId = 3;
  embedding_index_->set_last_added_document_id(kCurrentDocumentId);
  ASSERT_THAT(embedding_index_->last_added_document_id(),
              Eq(kCurrentDocumentId));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndexingHandler> handler,
      EmbeddingIndexingHandler::Create(&fake_clock_, embedding_index_.get(),
                                       /*enable_embedding_index=*/true));

  // Handling document with kInvalidDocumentId should cause a failure, and both
  // index data and last_added_document_id should remain unchanged.
  EXPECT_THAT(
      handler->Handle(tokenized_document, kInvalidDocumentId,
                      /*old_document_id=*/kInvalidDocumentId,
                      /*recovery_mode=*/false, /*put_document_stats=*/nullptr),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(embedding_index_->last_added_document_id(),
              Eq(kCurrentDocumentId));
  // Check that the embedding index should be empty
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());

  // Recovery mode should get the same result.
  EXPECT_THAT(
      handler->Handle(tokenized_document, kInvalidDocumentId,
                      /*old_document_id=*/kInvalidDocumentId,
                      /*recovery_mode=*/true, /*put_document_stats=*/nullptr),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(embedding_index_->last_added_document_id(),
              Eq(kCurrentDocumentId));
  // Check that the embedding index should be empty
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
}

TEST_F(EmbeddingIndexingHandlerTest,
       HandleOutOfOrderDocumentIdShouldReturnInvalidArgumentError) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kPropertyTitle), "title")
          .AddVectorProperty(std::string(kPropertyTitleEmbedding),
                             CreateVector("model", {0.1, 0.2, 0.3}))
          .AddStringProperty(std::string(kPropertyBody), "body")
          .AddVectorProperty(std::string(kPropertyBodyEmbedding),
                             CreateVector("model", {0.4, 0.5, 0.6}),
                             CreateVector("model", {0.7, 0.8, 0.9}))
          .AddVectorProperty(std::string(kPropertyNonIndexableEmbedding),
                             CreateVector("model", {1.1, 1.2, 1.3}))
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                std::move(document)));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result,
      document_store_->Put(tokenized_document.document()));
  DocumentId document_id = put_result.new_document_id;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndexingHandler> handler,
      EmbeddingIndexingHandler::Create(&fake_clock_, embedding_index_.get(),
                                       /*enable_embedding_index=*/true));

  // Handling document with document_id == last_added_document_id should cause a
  // failure, and both index data and last_added_document_id should remain
  // unchanged.
  embedding_index_->set_last_added_document_id(document_id);
  ASSERT_THAT(embedding_index_->last_added_document_id(), Eq(document_id));
  EXPECT_THAT(handler->Handle(
                  tokenized_document, document_id, put_result.old_document_id,
                  /*recovery_mode=*/false, /*put_document_stats=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(embedding_index_->last_added_document_id(), Eq(document_id));

  // Check that the embedding index should be empty
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());

  // Handling document with document_id < last_added_document_id should cause a
  // failure, and both index data and last_added_document_id should remain
  // unchanged.
  embedding_index_->set_last_added_document_id(document_id + 1);
  ASSERT_THAT(embedding_index_->last_added_document_id(), Eq(document_id + 1));
  EXPECT_THAT(handler->Handle(
                  tokenized_document, document_id, put_result.old_document_id,
                  /*recovery_mode=*/false, /*put_document_stats=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(embedding_index_->last_added_document_id(), Eq(document_id + 1));

  // Check that the embedding index should be empty
  EXPECT_THAT(GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                        /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_TRUE(embedding_index_->is_empty());
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()), IsEmpty());
}

TEST_F(EmbeddingIndexingHandlerTest,
       HandleRecoveryModeShouldIgnoreDocsLELastAddedDocId) {
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("icing", "fake_type/1")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kPropertyTitle), "title one")
          .AddVectorProperty(std::string(kPropertyTitleEmbedding),
                             CreateVector("model", {0.1, 0.2, 0.3}))
          .AddStringProperty(std::string(kPropertyBody), "body one")
          .AddVectorProperty(std::string(kPropertyBodyEmbedding),
                             CreateVector("model", {0.4, 0.5, 0.6}),
                             CreateVector("model", {0.7, 0.8, 0.9}))
          .AddVectorProperty(std::string(kPropertyNonIndexableEmbedding),
                             CreateVector("model", {1.1, 1.2, 1.3}))
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("icing", "fake_type/2")
          .SetSchema(std::string(kFakeType))
          .AddStringProperty(std::string(kPropertyTitle), "title two")
          .AddVectorProperty(std::string(kPropertyTitleEmbedding),
                             CreateVector("model", {10.1, 10.2, 10.3}))
          .AddStringProperty(std::string(kPropertyBody), "body two")
          .AddVectorProperty(std::string(kPropertyBodyEmbedding),
                             CreateVector("model", {10.4, 10.5, 10.6}),
                             CreateVector("model", {10.7, 10.8, 10.9}))
          .AddVectorProperty(std::string(kPropertyNonIndexableEmbedding),
                             CreateVector("model", {11.1, 11.2, 11.3}))
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document1,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                std::move(document1)));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_document2,
      TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                std::move(document2)));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(tokenized_document1.document()));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(tokenized_document2.document()));
  DocumentId document_id2 = put_result2.new_document_id;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingIndexingHandler> handler,
      EmbeddingIndexingHandler::Create(&fake_clock_, embedding_index_.get(),
                                       /*enable_embedding_index=*/true));

  // Handle document with document_id > last_added_document_id in recovery mode.
  // The handler should index this document and update last_added_document_id.
  EXPECT_THAT(
      handler->Handle(tokenized_document1, document_id1,
                      put_result1.old_document_id, /*recovery_mode=*/true,
                      /*put_document_stats=*/nullptr),
      IsOk());
  EXPECT_THAT(embedding_index_->last_added_document_id(), Eq(document_id1));

  // Check index
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(
          EmbeddingHit(BasicHit(kSectionIdBodyEmbedding, /*document_id=*/0),
                       /*location=*/0),
          EmbeddingHit(BasicHit(kSectionIdBodyEmbedding, /*document_id=*/0),
                       /*location=*/3),
          EmbeddingHit(BasicHit(kSectionIdTitleEmbedding, /*document_id=*/0),
                       /*location=*/6))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2, 0.3));

  // Handle document with document_id == last_added_document_id in recovery
  // mode. We should not get any error, but the handler should ignore the
  // document, so both index data and last_added_document_id should remain
  // unchanged.
  embedding_index_->set_last_added_document_id(document_id2);
  ASSERT_THAT(embedding_index_->last_added_document_id(), Eq(document_id2));
  EXPECT_THAT(
      handler->Handle(tokenized_document2, document_id2,
                      put_result2.old_document_id, /*recovery_mode=*/true,
                      /*put_document_stats=*/nullptr),
      IsOk());
  EXPECT_THAT(embedding_index_->last_added_document_id(), Eq(document_id2));

  // Check index
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(
          EmbeddingHit(BasicHit(kSectionIdBodyEmbedding, /*document_id=*/0),
                       /*location=*/0),
          EmbeddingHit(BasicHit(kSectionIdBodyEmbedding, /*document_id=*/0),
                       /*location=*/3),
          EmbeddingHit(BasicHit(kSectionIdTitleEmbedding, /*document_id=*/0),
                       /*location=*/6))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2, 0.3));

  // Handle document with document_id < last_added_document_id in recovery mode.
  // We should not get any error, but the handler should ignore the document, so
  // both index data and last_added_document_id should remain unchanged.
  embedding_index_->set_last_added_document_id(document_id2 + 1);
  ASSERT_THAT(embedding_index_->last_added_document_id(), Eq(document_id2 + 1));
  EXPECT_THAT(
      handler->Handle(tokenized_document2, document_id2,
                      put_result2.old_document_id, /*recovery_mode=*/true,
                      /*put_document_stats=*/nullptr),
      IsOk());
  EXPECT_THAT(embedding_index_->last_added_document_id(), Eq(document_id2 + 1));

  // Check index
  EXPECT_THAT(
      GetEmbeddingHitsFromIndex(embedding_index_.get(), /*dimension=*/3,
                                /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(
          EmbeddingHit(BasicHit(kSectionIdBodyEmbedding, /*document_id=*/0),
                       /*location=*/0),
          EmbeddingHit(BasicHit(kSectionIdBodyEmbedding, /*document_id=*/0),
                       /*location=*/3),
          EmbeddingHit(BasicHit(kSectionIdTitleEmbedding, /*document_id=*/0),
                       /*location=*/6))));
  EXPECT_THAT(GetRawEmbeddingDataFromIndex(embedding_index_.get()),
              ElementsAre(0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2, 0.3));
}

}  // namespace lib
}  // namespace icing
