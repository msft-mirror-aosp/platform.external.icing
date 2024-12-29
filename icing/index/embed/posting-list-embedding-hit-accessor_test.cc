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

#include "icing/index/embed/posting-list-embedding-hit-accessor.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/filesystem.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-accessor.h"
#include "icing/file/posting_list/posting-list-common.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/posting-list-embedding-hit-serializer.h"
#include "icing/index/hit/hit.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/hit-test-utils.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Lt;
using ::testing::SizeIs;

class PostingListEmbeddingHitAccessorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/test_dir";
    file_name_ = test_dir_ + "/test_file.idx.index";

    ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(test_dir_.c_str()));
    ASSERT_TRUE(filesystem_.CreateDirectoryRecursively(test_dir_.c_str()));

    serializer_ = std::make_unique<PostingListEmbeddingHitSerializer>();

    ICING_ASSERT_OK_AND_ASSIGN(
        FlashIndexStorage flash_index_storage,
        FlashIndexStorage::Create(file_name_, &filesystem_, serializer_.get()));
    flash_index_storage_ =
        std::make_unique<FlashIndexStorage>(std::move(flash_index_storage));
  }

  void TearDown() override {
    flash_index_storage_.reset();
    serializer_.reset();
    ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(test_dir_.c_str()));
  }

  Filesystem filesystem_;
  std::string test_dir_;
  std::string file_name_;
  std::unique_ptr<PostingListEmbeddingHitSerializer> serializer_;
  std::unique_ptr<FlashIndexStorage> flash_index_storage_;
};

TEST_F(PostingListEmbeddingHitAccessorTest, HitsAddAndRetrieveProperly) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  // Add some hits! Any hits!
  std::vector<EmbeddingHit> hits1 =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/1);
  for (const EmbeddingHit& hit : hits1) {
    ICING_ASSERT_OK(pl_accessor->PrependHit(hit));
  }
  PostingListAccessor::FinalizeResult result =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result.status);
  EXPECT_THAT(result.id.block_index(), Eq(1));
  EXPECT_THAT(result.id.posting_list_index(), Eq(0));

  // Retrieve some hits.
  ICING_ASSERT_OK_AND_ASSIGN(PostingListHolder pl_holder,
                             flash_index_storage_->GetPostingList(result.id));
  EXPECT_THAT(serializer_->GetHits(&pl_holder.posting_list),
              IsOkAndHolds(ElementsAreArray(hits1.rbegin(), hits1.rend())));
  EXPECT_THAT(pl_holder.next_block_index, Eq(kInvalidBlockIndex));
}

TEST_F(PostingListEmbeddingHitAccessorTest, PreexistingPLKeepOnSameBlock) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  // Add a single hit. This will fit in a min-sized posting list.
  EmbeddingHit hit1(BasicHit(/*section_id=*/1, /*document_id=*/0),
                    /*location=*/1);
  ICING_ASSERT_OK(pl_accessor->PrependHit(hit1));
  PostingListAccessor::FinalizeResult result1 =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result1.status);
  // Should have been allocated to the first block.
  EXPECT_THAT(result1.id.block_index(), Eq(1));
  EXPECT_THAT(result1.id.posting_list_index(), Eq(0));

  // Add one more hit. The minimum size for a posting list must be able to fit
  // at least two hits, so this should NOT cause the previous pl to be
  // reallocated.
  ICING_ASSERT_OK_AND_ASSIGN(
      pl_accessor,
      PostingListEmbeddingHitAccessor::CreateFromExisting(
          flash_index_storage_.get(), serializer_.get(), result1.id));
  EmbeddingHit hit2(BasicHit(/*section_id=*/1, /*document_id=*/0),
                    /*location=*/0);
  ICING_ASSERT_OK(pl_accessor->PrependHit(hit2));
  PostingListAccessor::FinalizeResult result2 =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result2.status);
  // Should have been allocated to the same posting list as the first hit.
  EXPECT_THAT(result2.id, Eq(result1.id));

  // The posting list at result2.id should hold all of the hits that have been
  // added.
  ICING_ASSERT_OK_AND_ASSIGN(PostingListHolder pl_holder,
                             flash_index_storage_->GetPostingList(result2.id));
  EXPECT_THAT(serializer_->GetHits(&pl_holder.posting_list),
              IsOkAndHolds(ElementsAre(hit2, hit1)));
}

TEST_F(PostingListEmbeddingHitAccessorTest, PreexistingPLReallocateToLargerPL) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  std::vector<EmbeddingHit> hits =
      CreateEmbeddingHits(/*num_hits=*/11, /*desired_byte_length=*/1);

  // Add 8 hits with a small posting list of 24 bytes. The first 7 hits will
  // be compressed to one byte each and will be able to fit in the 8 byte
  // padded region. The last hit will fit in one of the special hits. The
  // posting list will be ALMOST_FULL and can fit at most 2 more hits.
  for (int i = 0; i < 8; ++i) {
    ICING_ASSERT_OK(pl_accessor->PrependHit(hits[i]));
  }
  PostingListAccessor::FinalizeResult result1 =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result1.status);
  // Should have been allocated to the first block.
  EXPECT_THAT(result1.id.block_index(), Eq(1));
  EXPECT_THAT(result1.id.posting_list_index(), Eq(0));

  // Now let's add some more hits!
  ICING_ASSERT_OK_AND_ASSIGN(
      pl_accessor,
      PostingListEmbeddingHitAccessor::CreateFromExisting(
          flash_index_storage_.get(), serializer_.get(), result1.id));
  // The current posting list can fit at most 2 more hits.
  for (int i = 8; i < 10; ++i) {
    ICING_ASSERT_OK(pl_accessor->PrependHit(hits[i]));
  }
  PostingListAccessor::FinalizeResult result2 =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result2.status);
  // The 2 hits should still fit on the first block
  EXPECT_THAT(result1.id.block_index(), Eq(1));
  EXPECT_THAT(result1.id.posting_list_index(), Eq(0));

  // Add one more hit
  ICING_ASSERT_OK_AND_ASSIGN(
      pl_accessor,
      PostingListEmbeddingHitAccessor::CreateFromExisting(
          flash_index_storage_.get(), serializer_.get(), result2.id));
  // The current posting list should be FULL. Adding more hits should result in
  // these hits being moved to a larger posting list.
  ICING_ASSERT_OK(pl_accessor->PrependHit(hits[10]));
  PostingListAccessor::FinalizeResult result3 =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result3.status);
  // Should have been allocated to the second (new) block because the posting
  // list should have grown beyond the size that the first block maintains.
  EXPECT_THAT(result3.id.block_index(), Eq(2));
  EXPECT_THAT(result3.id.posting_list_index(), Eq(0));

  // The posting list at result3.id should hold all of the hits that have been
  // added.
  ICING_ASSERT_OK_AND_ASSIGN(PostingListHolder pl_holder,
                             flash_index_storage_->GetPostingList(result3.id));
  EXPECT_THAT(serializer_->GetHits(&pl_holder.posting_list),
              IsOkAndHolds(ElementsAreArray(hits.rbegin(), hits.rend())));
}

TEST_F(PostingListEmbeddingHitAccessorTest, MultiBlockChainsBlocksProperly) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  // Add some hits! Any hits!
  std::vector<EmbeddingHit> hits1 =
      CreateEmbeddingHits(/*num_hits=*/5000, /*desired_byte_length=*/1);
  for (const EmbeddingHit& hit : hits1) {
    ICING_ASSERT_OK(pl_accessor->PrependHit(hit));
  }
  PostingListAccessor::FinalizeResult result1 =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result1.status);
  PostingListIdentifier second_block_id = result1.id;
  // Should have been allocated to the second block, which holds a max-sized
  // posting list.
  EXPECT_THAT(second_block_id, Eq(PostingListIdentifier(
                                   /*block_index=*/2, /*posting_list_index=*/0,
                                   /*posting_list_index_bits=*/0)));

  // Now let's retrieve them!
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListHolder pl_holder,
      flash_index_storage_->GetPostingList(second_block_id));
  // This pl_holder will only hold a posting list with the hits that didn't fit
  // on the first block.
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<EmbeddingHit> second_block_hits,
                             serializer_->GetHits(&pl_holder.posting_list));
  ASSERT_THAT(second_block_hits, SizeIs(Lt(hits1.size())));
  auto first_block_hits_start = hits1.rbegin() + second_block_hits.size();
  EXPECT_THAT(second_block_hits,
              ElementsAreArray(hits1.rbegin(), first_block_hits_start));

  // Now retrieve all of the hits that were on the first block.
  uint32_t first_block_id = pl_holder.next_block_index;
  EXPECT_THAT(first_block_id, Eq(1));

  PostingListIdentifier pl_id(first_block_id, /*posting_list_index=*/0,
                              /*posting_list_index_bits=*/0);
  ICING_ASSERT_OK_AND_ASSIGN(pl_holder,
                             flash_index_storage_->GetPostingList(pl_id));
  EXPECT_THAT(
      serializer_->GetHits(&pl_holder.posting_list),
      IsOkAndHolds(ElementsAreArray(first_block_hits_start, hits1.rend())));
}

TEST_F(PostingListEmbeddingHitAccessorTest,
       PreexistingMultiBlockReusesBlocksProperly) {
  std::vector<EmbeddingHit> hits =
      CreateEmbeddingHits(/*num_hits=*/5050, /*desired_byte_length=*/1);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  // Add some hits! Any hits!
  for (int i = 0; i < 5000; ++i) {
    ICING_ASSERT_OK(pl_accessor->PrependHit(hits[i]));
  }
  PostingListAccessor::FinalizeResult result1 =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result1.status);
  PostingListIdentifier first_add_id = result1.id;
  EXPECT_THAT(first_add_id, Eq(PostingListIdentifier(
                                /*block_index=*/2, /*posting_list_index=*/0,
                                /*posting_list_index_bits=*/0)));

  // Now add a couple more hits. These should fit on the existing, not full
  // second block.
  ICING_ASSERT_OK_AND_ASSIGN(
      pl_accessor,
      PostingListEmbeddingHitAccessor::CreateFromExisting(
          flash_index_storage_.get(), serializer_.get(), first_add_id));
  for (int i = 5000; i < hits.size(); ++i) {
    ICING_ASSERT_OK(pl_accessor->PrependHit(hits[i]));
  }
  PostingListAccessor::FinalizeResult result2 =
      std::move(*pl_accessor).Finalize();
  ICING_EXPECT_OK(result2.status);
  PostingListIdentifier second_add_id = result2.id;
  EXPECT_THAT(second_add_id, Eq(first_add_id));

  // We should be able to retrieve all 5050 hits.
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListHolder pl_holder,
      flash_index_storage_->GetPostingList(second_add_id));
  // This pl_holder will only hold a posting list with the hits that didn't fit
  // on the first block.
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<EmbeddingHit> second_block_hits,
                             serializer_->GetHits(&pl_holder.posting_list));
  ASSERT_THAT(second_block_hits, SizeIs(Lt(hits.size())));
  auto first_block_hits_start = hits.rbegin() + second_block_hits.size();
  EXPECT_THAT(second_block_hits,
              ElementsAreArray(hits.rbegin(), first_block_hits_start));

  // Now retrieve all of the hits that were on the first block.
  uint32_t first_block_id = pl_holder.next_block_index;
  EXPECT_THAT(first_block_id, Eq(1));

  PostingListIdentifier pl_id(first_block_id, /*posting_list_index=*/0,
                              /*posting_list_index_bits=*/0);
  ICING_ASSERT_OK_AND_ASSIGN(pl_holder,
                             flash_index_storage_->GetPostingList(pl_id));
  EXPECT_THAT(
      serializer_->GetHits(&pl_holder.posting_list),
      IsOkAndHolds(ElementsAreArray(first_block_hits_start, hits.rend())));
}

TEST_F(PostingListEmbeddingHitAccessorTest, InvalidHitReturnsInvalidArgument) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  EmbeddingHit invalid_hit(EmbeddingHit::kInvalidValue);
  EXPECT_THAT(pl_accessor->PrependHit(invalid_hit),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(PostingListEmbeddingHitAccessorTest,
       HitsNotDecreasingReturnsInvalidArgument) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  EmbeddingHit hit1(BasicHit(/*section_id=*/3, /*document_id=*/1),
                    /*location=*/5);
  ICING_ASSERT_OK(pl_accessor->PrependHit(hit1));

  EmbeddingHit hit2(BasicHit(/*section_id=*/6, /*document_id=*/1),
                    /*location=*/5);
  EXPECT_THAT(pl_accessor->PrependHit(hit2),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EmbeddingHit hit3(BasicHit(/*section_id=*/2, /*document_id=*/0),
                    /*location=*/5);
  EXPECT_THAT(pl_accessor->PrependHit(hit3),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EmbeddingHit hit4(BasicHit(/*section_id=*/3, /*document_id=*/1),
                    /*location=*/6);
  EXPECT_THAT(pl_accessor->PrependHit(hit4),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(PostingListEmbeddingHitAccessorTest, NewPostingListNoHitsAdded) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  PostingListAccessor::FinalizeResult result1 =
      std::move(*pl_accessor).Finalize();
  EXPECT_THAT(result1.status,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(PostingListEmbeddingHitAccessorTest, PreexistingPostingListNoHitsAdded) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::Create(flash_index_storage_.get(),
                                              serializer_.get()));
  EmbeddingHit hit1(BasicHit(/*section_id=*/3, /*document_id=*/1),
                    /*location=*/5);
  ICING_ASSERT_OK(pl_accessor->PrependHit(hit1));
  PostingListAccessor::FinalizeResult result1 =
      std::move(*pl_accessor).Finalize();
  ICING_ASSERT_OK(result1.status);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor2,
      PostingListEmbeddingHitAccessor::CreateFromExisting(
          flash_index_storage_.get(), serializer_.get(), result1.id));
  PostingListAccessor::FinalizeResult result2 =
      std::move(*pl_accessor2).Finalize();
  ICING_ASSERT_OK(result2.status);
}

}  // namespace

}  // namespace lib
}  // namespace icing