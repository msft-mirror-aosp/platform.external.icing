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

#ifndef ICING_FILE_POSTING_LIST_POSTING_LIST_ACCESSOR_H_
#define ICING_FILE_POSTING_LIST_POSTING_LIST_ACCESSOR_H_

#include <cstdint>
#include <memory>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/file/posting_list/posting-list-used.h"

namespace icing {
namespace lib {

// This class serves to:
//  1. Expose PostingListUseds to clients of FlashIndexStorage
//  2. Ensure the corresponding instance of IndexBlock has the same lifecycle as
//     the instance of PostingListUsed that the client has access to, while
//     not exposing IndexBlock's api surface.
//  3. Ensure that PostingListUseds can only be freed by calling methods which
//     will also properly maintain the FlashIndexStorage free list and prevent
//     callers from modifying the Posting List after freeing.
class PostingListAccessor {
 public:
  virtual ~PostingListAccessor() = default;

  struct FinalizeResult {
    //   - OK on success
    //   - INVALID_ARGUMENT if there was no pre-existing posting list and no
    //     data were added
    //   - RESOURCE_EXHAUSTED error if unable to grow the index to allocate a
    //     new posting list.
    libtextclassifier3::Status status;
    // Id of the posting list chain that was finalized. Guaranteed to be valid
    // if status is OK. May be valid if status is non-OK, but previous blocks
    // were written.
    PostingListIdentifier id;

    explicit FinalizeResult(libtextclassifier3::Status status_in,
                            PostingListIdentifier id_in)
        : status(std::move(status_in)), id(std::move(id_in)) {}
  };
  // Write all accumulated data to storage.
  //
  // If accessor points to a posting list chain with multiple posting lists in
  // the chain and unable to write the last posting list in the chain, Finalize
  // will return the error and also populate id with the id of the
  // second-to-last posting list.
  FinalizeResult Finalize() &&;

  virtual PostingListUsedSerializer* GetSerializer() = 0;

 protected:
  explicit PostingListAccessor(
      FlashIndexStorage* storage,
      std::unique_ptr<uint8_t[]> posting_list_buffer_array,
      PostingListUsed posting_list_buffer)
      : storage_(storage),
        prev_block_identifier_(PostingListIdentifier::kInvalid),
        posting_list_buffer_array_(std::move(posting_list_buffer_array)),
        posting_list_buffer_(std::move(posting_list_buffer)),
        has_reached_posting_list_chain_end_(false) {}

  // Flushes preexisting_posting_list_ to disk if it's a max-sized posting list
  // and populates prev_block_identifier.
  // If it's not a max-sized posting list, moves the contents of
  // preexisting_posting_list_ to posting_list_buffer_ and frees
  // preexisting_posting_list_.
  // Sets preexisting_posting_list_ to nullptr.
  void FlushPreexistingPostingList();

  // Flushes posting_list_buffer_ to a max-sized posting list on disk, setting
  // its next pointer to prev_block_identifier_ and updating
  // prev_block_identifier_ to point to the just-written posting list.
  libtextclassifier3::Status FlushInMemoryPostingList();

  // Frees all posting lists in the posting list chain starting at
  // prev_block_identifier_.
  libtextclassifier3::Status FreePostingListChain();

  FlashIndexStorage* storage_;  // Does not own.

  // The PostingListIdentifier of the first max-sized posting list in the
  // posting list chain or PostingListIdentifier::kInvalid if there is no
  // posting list chain.
  PostingListIdentifier prev_block_identifier_;

  // An editor to an existing posting list on disk. If available (non-NULL),
  // we'll try to add all data to this posting list. Once this posting list
  // fills up, we'll either 1) chain it (if a max-sized posting list) and put
  // future data in posting_list_buffer_ or 2) copy all of its data into
  // posting_list_buffer_ and free this pl (if not a max-sized posting list).
  // TODO(tjbarron) provide a benchmark to demonstrate the effects that re-using
  // existing posting lists has on latency.
  std::unique_ptr<PostingListHolder> preexisting_posting_list_;

  // In-memory posting list used to buffer data before writing them to the
  // smallest on-disk posting list that will fit them.
  // posting_list_buffer_array_ owns the memory region that posting_list_buffer_
  // interprets. Therefore, posting_list_buffer_array_ must have the same
  // lifecycle as posting_list_buffer_.
  std::unique_ptr<uint8_t[]> posting_list_buffer_array_;
  PostingListUsed posting_list_buffer_;

  bool has_reached_posting_list_chain_end_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_FILE_POSTING_LIST_POSTING_LIST_ACCESSOR_H_
