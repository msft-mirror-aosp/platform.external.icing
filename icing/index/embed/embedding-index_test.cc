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
#include "icing/file/filesystem.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/posting-list-embedding-hit-accessor.h"
#include "icing/index/hit/hit.h"
#include "icing/proto/document.pb.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Test;

class EmbeddingIndexTest : public Test {
 protected:
  void SetUp() override {
    embedding_index_dir_ = GetTestTempDir() + "/embedding_index_test";
    ICING_ASSERT_OK_AND_ASSIGN(
        embedding_index_,
        EmbeddingIndex::Create(&filesystem_, embedding_index_dir_));
  }

  void TearDown() override {
    embedding_index_.reset();
    filesystem_.DeleteDirectoryRecursively(embedding_index_dir_.c_str());
  }

  libtextclassifier3::StatusOr<std::vector<EmbeddingHit>> GetHits(
      uint32_t dimension, std::string_view model_signature) {
    std::vector<EmbeddingHit> hits;

    libtextclassifier3::StatusOr<
        std::unique_ptr<PostingListEmbeddingHitAccessor>>
        pl_accessor_or =
            embedding_index_->GetAccessor(dimension, model_signature);
    std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor;
    if (pl_accessor_or.ok()) {
      pl_accessor = std::move(pl_accessor_or).ValueOrDie();
    } else if (absl_ports::IsNotFound(pl_accessor_or.status())) {
      return hits;
    } else {
      return std::move(pl_accessor_or).status();
    }

    while (true) {
      ICING_ASSIGN_OR_RETURN(std::vector<EmbeddingHit> batch,
                             pl_accessor->GetNextHitsBatch());
      if (batch.empty()) {
        return hits;
      }
      hits.insert(hits.end(), batch.begin(), batch.end());
    }
  }

  std::vector<float> GetRawEmbeddingData() {
    return std::vector<float>(embedding_index_->GetRawEmbeddingData(),
                              embedding_index_->GetRawEmbeddingData() +
                                  embedding_index_->GetTotalVectorSize());
  }

  Filesystem filesystem_;
  std::string embedding_index_dir_;
  std::unique_ptr<EmbeddingIndex> embedding_index_;
};

TEST_F(EmbeddingIndexTest, AddSingleEmbedding) {
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(
      GetHits(/*dimension=*/3, /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingData(), ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, AddMultipleEmbeddingsInTheSameSection) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/3))));
  EXPECT_THAT(GetRawEmbeddingData(),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, HitsWithLowerSectionIdReturnedFirst) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/5, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/2, /*document_id=*/0), vector2));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/2, /*document_id=*/0),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/5, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingData(),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, HitsWithHigherDocumentIdReturnedFirst) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector2));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingData(),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);
}

TEST_F(EmbeddingIndexTest, AddEmbeddingsFromDifferentModels) {
  PropertyProto::VectorProto vector1 = CreateVector("model1", {0.1, 0.2});
  PropertyProto::VectorProto vector2 =
      CreateVector("model2", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetHits(/*dimension=*/2, /*model_signature=*/"model1"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model2"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/2))));
  EXPECT_THAT(
      GetHits(/*dimension=*/5, /*model_signature=*/"non-existent-model"),
      IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetRawEmbeddingData(), ElementsAre(0.1, 0.2, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest,
       AddEmbeddingsWithSameSignatureButDifferentDimension) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(0);

  EXPECT_THAT(GetHits(/*dimension=*/2, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/2))));
  EXPECT_THAT(GetRawEmbeddingData(), ElementsAre(0.1, 0.2, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, ClearIndex) {
  // Loop the same logic twice to make sure that clear works as expected, and
  // the index is still valid after clearing.
  for (int i = 0; i < 2; i++) {
    PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
    PropertyProto::VectorProto vector2 =
        CreateVector("model", {-0.1, -0.2, -0.3});
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/1, /*document_id=*/0), vector1));
    ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
        BasicHit(/*section_id=*/2, /*document_id=*/1), vector2));
    ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
    embedding_index_->set_last_added_document_id(1);

    EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
                IsOkAndHolds(ElementsAre(
                    EmbeddingHit(BasicHit(/*section_id=*/2, /*document_id=*/1),
                                 /*location=*/3),
                    EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                                 /*location=*/0))));
    EXPECT_THAT(GetRawEmbeddingData(),
                ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

    // Check that clear works as expected.
    ICING_ASSERT_OK(embedding_index_->Clear());
    EXPECT_THAT(GetRawEmbeddingData(), IsEmpty());
    EXPECT_EQ(embedding_index_->last_added_document_id(), kInvalidDocumentId);
  }
}

TEST_F(EmbeddingIndexTest, EmptyCommitIsOk) {
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  EXPECT_THAT(GetRawEmbeddingData(), IsEmpty());
}

TEST_F(EmbeddingIndexTest, MultipleCommits) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/3),
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingData(),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
}

TEST_F(EmbeddingIndexTest,
       InvalidCommit_SectionIdCanOnlyDecreaseForSingleDocument) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), vector2));
  // Posting list with delta encoding can only allow decreasing values.
  EXPECT_THAT(embedding_index_->CommitBufferToIndex(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(EmbeddingIndexTest, InvalidCommit_DocumentIdCanOnlyIncrease) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector1));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());

  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector2));
  // Posting list with delta encoding can only allow decreasing values, which
  // means document ids must be committed increasingly, since document ids are
  // inverted in hit values.
  EXPECT_THAT(embedding_index_->CommitBufferToIndex(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(EmbeddingIndexTest, EmptyOptimizeIsOk) {
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{},
      /*new_last_added_document_id=*/kInvalidDocumentId));
  EXPECT_THAT(GetRawEmbeddingData(), IsEmpty());
}

TEST_F(EmbeddingIndexTest, OptimizeSingleEmbeddingSingleDocument) {
  PropertyProto::VectorProto vector = CreateVector("model", {0.1, 0.2, 0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), vector));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Before optimize
  EXPECT_THAT(
      GetHits(/*dimension=*/3, /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/2), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingData(), ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize without deleting any documents, and check that the index is
  // not changed.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{0, 1, 2},
      /*new_last_added_document_id=*/2));
  EXPECT_THAT(
      GetHits(/*dimension=*/3, /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/2), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingData(), ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize to map document id 2 to 1, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{0, kInvalidDocumentId, 1},
      /*new_last_added_document_id=*/1));
  EXPECT_THAT(
      GetHits(/*dimension=*/3, /*model_signature=*/"model"),
      IsOkAndHolds(ElementsAre(EmbeddingHit(
          BasicHit(/*section_id=*/0, /*document_id=*/1), /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingData(), ElementsAre(0.1, 0.2, 0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete the document.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetRawEmbeddingData(), IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeMultipleEmbeddingsSingleDocument) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), vector1));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/2), vector2));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(2);

  // Before optimize
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/3))));
  EXPECT_THAT(GetRawEmbeddingData(),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize without deleting any documents, and check that the index is
  // not changed.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{0, 1, 2},
      /*new_last_added_document_id=*/2));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/2),
                               /*location=*/3))));
  EXPECT_THAT(GetRawEmbeddingData(),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 2);

  // Run optimize to map document id 2 to 1, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{0, kInvalidDocumentId, 1},
      /*new_last_added_document_id=*/1));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/3))));
  EXPECT_THAT(GetRawEmbeddingData(),
              ElementsAre(0.1, 0.2, 0.3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize to delete the document.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetRawEmbeddingData(), IsEmpty());
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeMultipleEmbeddingsMultipleDocument) {
  PropertyProto::VectorProto vector1 = CreateVector("model", {0.1, 0.2, 0.3});
  PropertyProto::VectorProto vector2 = CreateVector("model", {1, 2, 3});
  PropertyProto::VectorProto vector3 =
      CreateVector("model", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/0), vector2));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector3));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  // Before optimize
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/6),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0),
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                               /*location=*/3))));
  EXPECT_THAT(GetRawEmbeddingData(),
              ElementsAre(0.1, 0.2, 0.3, 1, 2, 3, -0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 1);

  // Run optimize without deleting any documents. It is expected to see that the
  // raw embedding data is rearranged, since during index transfer, embedding
  // vectors from higher document ids are added first.
  //
  // Also keep in mind that once the raw data is rearranged, calling another
  // Optimize subsequently will not change the raw data again.
  for (int i = 0; i < 2; i++) {
    ICING_ASSERT_OK(embedding_index_->Optimize(
        /*document_id_old_to_new=*/{0, 1},
        /*new_last_added_document_id=*/1));
    EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
                IsOkAndHolds(ElementsAre(
                    EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                                 /*location=*/0),
                    EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                                 /*location=*/3),
                    EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/0),
                                 /*location=*/6))));
    EXPECT_THAT(GetRawEmbeddingData(),
                ElementsAre(-0.1, -0.2, -0.3, 0.1, 0.2, 0.3, 1, 2, 3));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 1);
  }

  // Run optimize to delete document 0, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{kInvalidDocumentId, 0},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetRawEmbeddingData(), ElementsAre(-0.1, -0.2, -0.3));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

TEST_F(EmbeddingIndexTest, OptimizeEmbeddingsFromDifferentModels) {
  PropertyProto::VectorProto vector1 = CreateVector("model1", {0.1, 0.2});
  PropertyProto::VectorProto vector2 = CreateVector("model1", {1, 2});
  PropertyProto::VectorProto vector3 =
      CreateVector("model2", {-0.1, -0.2, -0.3});
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/0), vector1));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/0, /*document_id=*/1), vector2));
  ICING_ASSERT_OK(embedding_index_->BufferEmbedding(
      BasicHit(/*section_id=*/1, /*document_id=*/1), vector3));
  ICING_ASSERT_OK(embedding_index_->CommitBufferToIndex());
  embedding_index_->set_last_added_document_id(1);

  // Before optimize
  EXPECT_THAT(GetHits(/*dimension=*/2, /*model_signature=*/"model1"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                               /*location=*/2),
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model2"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/1),
                               /*location=*/4))));
  EXPECT_THAT(GetRawEmbeddingData(),
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
    ICING_ASSERT_OK(embedding_index_->Optimize(
        /*document_id_old_to_new=*/{0, 1},
        /*new_last_added_document_id=*/1));
    EXPECT_THAT(GetHits(/*dimension=*/2, /*model_signature=*/"model1"),
                IsOkAndHolds(ElementsAre(
                    EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/1),
                                 /*location=*/0),
                    EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                                 /*location=*/2))));
    EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model2"),
                IsOkAndHolds(ElementsAre(
                    EmbeddingHit(BasicHit(/*section_id=*/1, /*document_id=*/1),
                                 /*location=*/4))));
    EXPECT_THAT(GetRawEmbeddingData(),
                ElementsAre(1, 2, 0.1, 0.2, -0.1, -0.2, -0.3));
    EXPECT_EQ(embedding_index_->last_added_document_id(), 1);
  }

  // Run optimize to delete document 1, and check that the index is
  // updated correctly.
  ICING_ASSERT_OK(embedding_index_->Optimize(
      /*document_id_old_to_new=*/{0, kInvalidDocumentId},
      /*new_last_added_document_id=*/0));
  EXPECT_THAT(GetHits(/*dimension=*/2, /*model_signature=*/"model1"),
              IsOkAndHolds(ElementsAre(
                  EmbeddingHit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                               /*location=*/0))));
  EXPECT_THAT(GetHits(/*dimension=*/3, /*model_signature=*/"model2"),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(GetRawEmbeddingData(), ElementsAre(0.1, 0.2));
  EXPECT_EQ(embedding_index_->last_added_document_id(), 0);
}

}  // namespace
}  // namespace lib
}  // namespace icing
