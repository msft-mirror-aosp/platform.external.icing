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

#ifndef ICING_INDEX_EMBED_POSTING_LIST_EMBEDDING_HIT_ACCESSOR_H_
#define ICING_INDEX_EMBED_POSTING_LIST_EMBEDDING_HIT_ACCESSOR_H_

#include <memory>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-accessor.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/file/posting_list/posting-list-used.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/posting-list-embedding-hit-serializer.h"

namespace icing {
namespace lib {

// This class is used to provide a simple abstraction for adding hits to posting
// lists. PostingListEmbeddingHitAccessor handles 1) selection of properly-sized
// posting lists for the accumulated hits during Finalize() and 2) chaining of
// max-sized posting lists.
class PostingListEmbeddingHitAccessor : public PostingListAccessor {
 public:
  // Creates an empty PostingListEmbeddingHitAccessor.
  //
  // RETURNS:
  //   - On success, a valid unique_ptr instance of
  //   PostingListEmbeddingHitAccessor
  //   - INVALID_ARGUMENT error if storage has an invalid block_size.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<PostingListEmbeddingHitAccessor>>
  Create(FlashIndexStorage* storage,
         PostingListEmbeddingHitSerializer* serializer);

  // Create a PostingListEmbeddingHitAccessor with an existing posting list
  // identified by existing_posting_list_id.
  //
  // The PostingListEmbeddingHitAccessor will add hits to this posting list
  // until it is necessary either to 1) chain the posting list (if it is
  // max-sized) or 2) move its hits to a larger posting list.
  //
  // RETURNS:
  //   - On success, a valid unique_ptr instance of
  //   PostingListEmbeddingHitAccessor
  //   - INVALID_ARGUMENT if storage has an invalid block_size.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<PostingListEmbeddingHitAccessor>>
  CreateFromExisting(FlashIndexStorage* storage,
                     PostingListEmbeddingHitSerializer* serializer,
                     PostingListIdentifier existing_posting_list_id);

  PostingListSerializer* GetSerializer() override { return serializer_; }

  // Retrieve the next batch of hits for the posting list chain
  //
  // RETURNS:
  //   - On success, a vector of hits in the posting list chain
  //   - INTERNAL if called on an instance of PostingListEmbeddingHitAccessor
  //     that was created via PostingListEmbeddingHitAccessor::Create, if unable
  //     to read the next posting list in the chain or if the posting list has
  //     been corrupted somehow.
  libtextclassifier3::StatusOr<std::vector<EmbeddingHit>> GetNextHitsBatch();

  // Prepend one hit. This may result in flushing the posting list to disk (if
  // the PostingListEmbeddingHitAccessor holds a max-sized posting list that is
  // full) or freeing a pre-existing posting list if it is too small to fit all
  // hits necessary.
  //
  // RETURNS:
  //   - OK, on success
  //   - INVALID_ARGUMENT if !hit.is_valid() or if hit is not less than the
  //   previously added hit.
  //   - RESOURCE_EXHAUSTED error if unable to grow the index to allocate a new
  //   posting list.
  libtextclassifier3::Status PrependHit(const EmbeddingHit& hit);

 private:
  explicit PostingListEmbeddingHitAccessor(
      FlashIndexStorage* storage, PostingListEmbeddingHitSerializer* serializer,
      PostingListUsed in_memory_posting_list)
      : PostingListAccessor(storage, std::move(in_memory_posting_list)),
        serializer_(serializer) {}

  PostingListEmbeddingHitSerializer* serializer_;  // Does not own.
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_POSTING_LIST_EMBEDDING_HIT_ACCESSOR_H_
