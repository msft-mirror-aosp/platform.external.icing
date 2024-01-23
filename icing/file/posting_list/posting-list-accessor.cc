// Copyright (C) 2022 Google LLC
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

#include "icing/file/posting_list/posting-list-accessor.h"

#include <cstdint>
#include <memory>

#include "icing/absl_ports/canonical_errors.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/index-block.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/file/posting_list/posting-list-used.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

void PostingListAccessor::FlushPreexistingPostingList() {
  if (preexisting_posting_list_->block.max_num_posting_lists() == 1) {
    // If this is a max-sized posting list, then just keep track of the id for
    // chaining. It'll be flushed to disk when preexisting_posting_list_ is
    // destructed.
    prev_block_identifier_ = preexisting_posting_list_->id;
  } else {
    // If this is NOT a max-sized posting list, then our data have outgrown this
    // particular posting list. Move the data into the in-memory posting list
    // and free this posting list.
    //
    // Move will always succeed since posting_list_buffer_ is max_pl_bytes.
    GetSerializer()->MoveFrom(/*dst=*/&posting_list_buffer_,
                              /*src=*/&preexisting_posting_list_->posting_list);

    // Now that all the contents of this posting list have been copied, there's
    // no more use for it. Make it available to be used for another posting
    // list.
    storage_->FreePostingList(std::move(*preexisting_posting_list_));
  }
  preexisting_posting_list_.reset();
}

libtextclassifier3::Status PostingListAccessor::FlushInMemoryPostingList() {
  // We exceeded max_pl_bytes(). Need to flush posting_list_buffer_ and update
  // the chain.
  uint32_t max_posting_list_bytes = IndexBlock::CalculateMaxPostingListBytes(
      storage_->block_size(), GetSerializer()->GetDataTypeBytes());
  ICING_ASSIGN_OR_RETURN(PostingListHolder holder,
                         storage_->AllocatePostingList(max_posting_list_bytes));
  holder.block.set_next_block_index(prev_block_identifier_.block_index());
  prev_block_identifier_ = holder.id;
  return GetSerializer()->MoveFrom(/*dst=*/&holder.posting_list,
                                   /*src=*/&posting_list_buffer_);
}

PostingListAccessor::FinalizeResult PostingListAccessor::Finalize() && {
  if (preexisting_posting_list_ != nullptr) {
    // Our data are already in an existing posting list. Nothing else to do, but
    // return its id.
    return FinalizeResult(libtextclassifier3::Status::OK,
                          preexisting_posting_list_->id);
  }
  if (GetSerializer()->GetBytesUsed(&posting_list_buffer_) <= 0) {
    return FinalizeResult(absl_ports::InvalidArgumentError(
                              "Can't finalize an empty PostingListAccessor. "
                              "There's nothing to Finalize!"),
                          PostingListIdentifier::kInvalid);
  }
  uint32_t posting_list_bytes =
      GetSerializer()->GetMinPostingListSizeToFit(&posting_list_buffer_);
  if (prev_block_identifier_.is_valid()) {
    posting_list_bytes = IndexBlock::CalculateMaxPostingListBytes(
        storage_->block_size(), GetSerializer()->GetDataTypeBytes());
  }
  auto holder_or = storage_->AllocatePostingList(posting_list_bytes);
  if (!holder_or.ok()) {
    return FinalizeResult(std::move(holder_or).status(),
                          prev_block_identifier_);
  }
  PostingListHolder holder = std::move(holder_or).ValueOrDie();
  if (prev_block_identifier_.is_valid()) {
    holder.block.set_next_block_index(prev_block_identifier_.block_index());
  }

  // Move to allocated area. This should never actually return an error. We know
  // that editor.posting_list() is valid because it wouldn't have successfully
  // returned by AllocatePostingList if it wasn't. We know posting_list_buffer_
  // is valid because we created it in-memory. And finally, we know that the
  // data from posting_list_buffer_ will fit in editor.posting_list() because we
  // requested it be at at least posting_list_bytes large.
  auto status = GetSerializer()->MoveFrom(/*dst=*/&holder.posting_list,
                                          /*src=*/&posting_list_buffer_);
  if (!status.ok()) {
    return FinalizeResult(std::move(status), prev_block_identifier_);
  }
  return FinalizeResult(libtextclassifier3::Status::OK, holder.id);
}

}  // namespace lib
}  // namespace icing
