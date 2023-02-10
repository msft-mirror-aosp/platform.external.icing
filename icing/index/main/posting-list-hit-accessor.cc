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

#include "icing/index/main/posting-list-hit-accessor.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "icing/absl_ports/canonical_errors.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/index-block.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/file/posting_list/posting-list-used.h"
#include "icing/index/main/posting-list-hit-serializer.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<PostingListHitAccessor>>
PostingListHitAccessor::Create(FlashIndexStorage *storage,
                               PostingListHitSerializer *serializer) {
  uint32_t max_posting_list_bytes = IndexBlock::CalculateMaxPostingListBytes(
      storage->block_size(), serializer->GetDataTypeBytes());
  std::unique_ptr<uint8_t[]> posting_list_buffer_array =
      std::make_unique<uint8_t[]>(max_posting_list_bytes);
  ICING_ASSIGN_OR_RETURN(
      PostingListUsed posting_list_buffer,
      PostingListUsed::CreateFromUnitializedRegion(
          serializer, posting_list_buffer_array.get(), max_posting_list_bytes));
  return std::unique_ptr<PostingListHitAccessor>(new PostingListHitAccessor(
      storage, serializer, std::move(posting_list_buffer_array),
      std::move(posting_list_buffer)));
}

libtextclassifier3::StatusOr<std::unique_ptr<PostingListHitAccessor>>
PostingListHitAccessor::CreateFromExisting(
    FlashIndexStorage *storage, PostingListHitSerializer *serializer,
    PostingListIdentifier existing_posting_list_id) {
  // Our posting_list_buffer_ will start as empty.
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<PostingListHitAccessor> pl_accessor,
                         Create(storage, serializer));
  ICING_ASSIGN_OR_RETURN(PostingListHolder holder,
                         storage->GetPostingList(existing_posting_list_id));
  pl_accessor->preexisting_posting_list_ =
      std::make_unique<PostingListHolder>(std::move(holder));
  return pl_accessor;
}

// Returns the next batch of hits for the provided posting list.
libtextclassifier3::StatusOr<std::vector<Hit>>
PostingListHitAccessor::GetNextHitsBatch() {
  if (preexisting_posting_list_ == nullptr) {
    if (has_reached_posting_list_chain_end_) {
      return std::vector<Hit>();
    }
    return absl_ports::FailedPreconditionError(
        "Cannot retrieve hits from a PostingListHitAccessor that was not "
        "created from a preexisting posting list.");
  }
  ICING_ASSIGN_OR_RETURN(
      std::vector<Hit> batch,
      serializer_->GetHits(&preexisting_posting_list_->posting_list));
  uint32_t next_block_index;
  // Posting lists will only be chained when they are max-sized, in which case
  // block.next_block_index() will point to the next block for the next posting
  // list. Otherwise, block.next_block_index() can be kInvalidBlockIndex or be
  // used to point to the next free list block, which is not relevant here.
  if (preexisting_posting_list_->block.max_num_posting_lists() == 1) {
    next_block_index = preexisting_posting_list_->block.next_block_index();
  } else {
    next_block_index = kInvalidBlockIndex;
  }
  if (next_block_index != kInvalidBlockIndex) {
    PostingListIdentifier next_posting_list_id(
        next_block_index, /*posting_list_index=*/0,
        preexisting_posting_list_->block.posting_list_index_bits());
    ICING_ASSIGN_OR_RETURN(PostingListHolder holder,
                           storage_->GetPostingList(next_posting_list_id));
    preexisting_posting_list_ =
        std::make_unique<PostingListHolder>(std::move(holder));
  } else {
    has_reached_posting_list_chain_end_ = true;
    preexisting_posting_list_.reset();
  }
  return batch;
}

libtextclassifier3::Status PostingListHitAccessor::PrependHit(const Hit &hit) {
  PostingListUsed &active_pl = (preexisting_posting_list_ != nullptr)
                                   ? preexisting_posting_list_->posting_list
                                   : posting_list_buffer_;
  libtextclassifier3::Status status = serializer_->PrependHit(&active_pl, hit);
  if (!absl_ports::IsResourceExhausted(status)) {
    return status;
  }
  // There is no more room to add hits to this current posting list! Therefore,
  // we need to either move those hits to a larger posting list or flush this
  // posting list and create another max-sized posting list in the chain.
  if (preexisting_posting_list_ != nullptr) {
    FlushPreexistingPostingList();
  } else {
    ICING_RETURN_IF_ERROR(FlushInMemoryPostingList());
  }

  // Re-add hit. Should always fit since we just cleared posting_list_buffer_.
  // It's fine to explicitly reference posting_list_buffer_ here because there's
  // no way of reaching this line while preexisting_posting_list_ is still in
  // use.
  return serializer_->PrependHit(&posting_list_buffer_, hit);
}

}  // namespace lib
}  // namespace icing
