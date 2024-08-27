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

#ifndef ICING_FILE_POSTING_LIST_POSTING_LIST_USED_H_
#define ICING_FILE_POSTING_LIST_POSTING_LIST_USED_H_

#include <cstdint>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"

namespace icing {
namespace lib {

class PostingListUsed;

// Interface for PostingListUsed data serialization and deserialization.
// - It contains several common methods used by lower level of posting list
//   management related classes (e.g. FlashIndexStorage, IndexBlock,
//   PostingListUsed, etc).
// - Higher level classes (e.g. MainIndex) create their desired serializers
//   according to the data type they're dealing with, and pass the instance down
//   to all posting list management related classes.
// - Data specific methods can also be implemented in each serializer. They
//   won't be used by posting list management related classes, but higher level
//   classes are able to call it and deal with the specific data type.
//
// E.g. main index stores 'Hit' data into posting lists.
// - MainIndex creates PostingListUsedHitSerializer instance and uses hit data
//   related methods to serialize/deserialize Hit data to/from posting lists.
// - FlashIndexStorage, IndexBlock, PostingListUsed use the serializer created
//   by MainIndex, but hold the reference/pointer in the interface format
//   (PostingListUsedSerializer) and only use common interface methods to manage
//   posting list.
class PostingListUsedSerializer {
 public:
  // Special data is either a DataType instance or data_start_offset.
  template <typename DataType>
  union SpecialData {
    explicit SpecialData(const DataType& data) : data_(data) {}

    explicit SpecialData(uint32_t data_start_offset)
        : data_start_offset_(data_start_offset) {}

    const DataType& data() const { return data_; }

    uint32_t data_start_offset() const { return data_start_offset_; }
    void set_data_start_offset(uint32_t data_start_offset) {
      data_start_offset_ = data_start_offset;
    }

   private:
    DataType data_;
    uint32_t data_start_offset_;
  } __attribute__((packed));

  static constexpr uint32_t kNumSpecialData = 2;

  virtual ~PostingListUsedSerializer() = default;

  // Returns byte size of the data type.
  virtual uint32_t GetDataTypeBytes() const = 0;

  // Returns minimum posting list size allowed.
  //
  // Note that min posting list size should also be large enough to store a
  // single PostingListIndex (for posting list management usage), so we have to
  // add static_assert in each serializer implementation.
  // E.g.
  // static constexpr uint32_t kMinPostingListSize = kSpecialHitsSize;
  // static_assert(sizeof(PostingListIndex) <= kMinPostingListSize, "");
  virtual uint32_t GetMinPostingListSize() const = 0;

  // Returns minimum size of posting list that can fit these used bytes
  // (see MoveFrom).
  virtual uint32_t GetMinPostingListSizeToFit(
      const PostingListUsed* posting_list_used) const = 0;

  // Returns bytes used by actual data.
  virtual uint32_t GetBytesUsed(
      const PostingListUsed* posting_list_used) const = 0;

  // Clears the posting list. It is usually used for initializing a newly
  // allocated (or reclaimed from free posting list chain) posting list.
  virtual void Clear(PostingListUsed* posting_list_used) const = 0;

  // Moves contents from posting list 'src' to 'dst'. Clears 'src'.
  //
  // RETURNS:
  //   - OK on success
  //   - INVALID_ARGUMENT if 'src' is not valid or 'src' is too large to fit in
  //       'dst'.
  //   - FAILED_PRECONDITION if 'dst' posting list is in a corrupted state.
  virtual libtextclassifier3::Status MoveFrom(PostingListUsed* dst,
                                              PostingListUsed* src) const = 0;
};

// A posting list with data in it. Layout depends on the serializer.
class PostingListUsed {
 public:
  // Creates a PostingListUsed that points to a buffer of size_in_bytes bytes.
  // 'Preexisting' means that posting_list_buffer was previously modified by
  // another instance of PostingListUsed.
  //
  // Caller owns the data buffer and must not free it while using a
  // PostingListUsed.
  //
  // RETURNS:
  //   - A valid PostingListUsed if successful
  //   - INVALID_ARGUMENT if posting_list_utils::IsValidPostingListSize check
  //       fails
  //   - FAILED_PRECONDITION if serializer or posting_list_buffer is null
  static libtextclassifier3::StatusOr<PostingListUsed>
  CreateFromPreexistingPostingListUsedRegion(
      PostingListUsedSerializer* serializer, void* posting_list_buffer,
      uint32_t size_in_bytes);

  // Creates a PostingListUsed that points to a buffer of size_in_bytes bytes
  // and initializes the content of the buffer so that the returned
  // PostingListUsed is empty.
  //
  // Caller owns the posting_list_buffer buffer and must not free it while using
  // a PostingListUsed.
  //
  // RETURNS:
  //   - A valid PostingListUsed if successful
  //   - INVALID_ARGUMENT if posting_list_utils::IsValidPostingListSize check
  //       fails
  //   - FAILED_PRECONDITION if serializer or posting_list_buffer is null
  static libtextclassifier3::StatusOr<PostingListUsed>
  CreateFromUnitializedRegion(PostingListUsedSerializer* serializer,
                              void* posting_list_buffer,
                              uint32_t size_in_bytes);

  uint8_t* posting_list_buffer() { return posting_list_buffer_; }
  const uint8_t* posting_list_buffer() const { return posting_list_buffer_; }

  uint32_t size_in_bytes() const { return size_in_bytes_; }

 private:
  explicit PostingListUsed(void* posting_list_buffer, uint32_t size_in_bytes)
      : posting_list_buffer_(static_cast<uint8_t*>(posting_list_buffer)),
        size_in_bytes_(size_in_bytes) {}

  // A byte array of size size_in_bytes_ containing encoded data for this
  // posting list.
  uint8_t* posting_list_buffer_;  // does not own!
  uint32_t size_in_bytes_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_FILE_POSTING_LIST_POSTING_LIST_USED_H_
