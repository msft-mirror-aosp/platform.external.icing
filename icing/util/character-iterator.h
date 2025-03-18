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

#ifndef ICING_UTIL_CHARACTER_ITERATOR_H_
#define ICING_UTIL_CHARACTER_ITERATOR_H_

#include <string>
#include <string_view>

#include "icing/legacy/core/icing-string-util.h"
#include "icing/util/i18n-utils.h"
#include "unicode/utypes.h"

namespace icing {
namespace lib {

class CharacterIterator {
 public:
  explicit CharacterIterator(std::string_view text)
      : text_(text),
        cached_current_char_(i18n_utils::kInvalidUChar32),
        utf8_index_(0),
        utf16_index_(0),
        utf32_index_(0) {}

  CharacterIterator() : utf8_index_(-1), utf16_index_(-1), utf32_index_(-1) {}

  // Returns the character that the iterator currently points to.
  // i18n_utils::kInvalidUChar32 if unable to read that character.
  //
  // REQUIRES: the instance is not in an undefined state (i.e. all previous
  //   calls succeeded).
  //
  // RETURNS:
  //   - Null character if the iterator is at the end of the text.
  //   - The character that the iterator currently points to, if the iterator is
  //     within the text.
  //   - i18n_utils::kInvalidUChar32, if unable to decode the character.
  UChar32 GetCurrentChar() const;

  // Moves current position to desired_utf8_index.
  // REQUIRES: 0 <= desired_utf8_index <= text_.length()
  bool MoveToUtf8(int desired_utf8_index);

  // Advances from current position to the character that includes the specified
  // UTF-8 index.
  //
  // desired_utf8_index should be in range [0, text_.length()]. Note that it is
  // allowed to point one index past the end (i.e. equals text_.length()), but
  // no further.
  //
  // REQUIRES:
  //   - The instance is not in an undefined state (i.e. all previous calls
  //     succeeded).
  //   - The current position is not ahead of desired_utf8_index, i.e.
  //     utf8_index() <= desired_utf8_index.
  //
  // RETURNS:
  //   - True if successfully advanced.
  //   - False otherwise. Also the iterator will be in an undefined state.
  bool AdvanceToUtf8(int desired_utf8_index);

  // Rewinds from current position to the character that includes the specified
  // UTF-8 index.
  // REQUIRES: 0 <= desired_utf8_index
  bool RewindToUtf8(int desired_utf8_index);

  // Moves current position to desired_utf16_index.
  // REQUIRES: 0 <= desired_utf16_index <= text_.utf16_length()
  bool MoveToUtf16(int desired_utf16_index);

  // Advances current position to desired_utf16_index.
  // REQUIRES: desired_utf16_index <= text_.utf16_length()
  // desired_utf16_index is allowed to point one index past the end, but no
  // further.
  bool AdvanceToUtf16(int desired_utf16_index);

  // Rewinds current position to desired_utf16_index.
  // REQUIRES: 0 <= desired_utf16_index
  bool RewindToUtf16(int desired_utf16_index);

  // Moves current position to desired_utf32_index.
  // REQUIRES: 0 <= desired_utf32_index <= text_.utf32_length()
  bool MoveToUtf32(int desired_utf32_index);

  // Advances current position to desired_utf32_index.
  // REQUIRES: desired_utf32_index <= text_.utf32_length()
  // desired_utf32_index is allowed to point one index past the end, but no
  // further.
  bool AdvanceToUtf32(int desired_utf32_index);

  // Rewinds current position to desired_utf32_index.
  // REQUIRES: 0 <= desired_utf32_index
  bool RewindToUtf32(int desired_utf32_index);

  bool is_valid() const {
    return text_.data() != nullptr && utf8_index_ >= 0 && utf16_index_ >= 0 &&
           utf32_index_ >= 0;
  }

  std::string_view text() const { return text_; }
  int utf8_index() const { return utf8_index_; }
  int utf16_index() const { return utf16_index_; }
  int utf32_index() const { return utf32_index_; }

  bool operator==(const CharacterIterator& rhs) const {
    // cached_current_char_ is just that: a cached value. As such, it's not
    // considered for equality.
    return text_ == rhs.text_ && utf8_index_ == rhs.utf8_index_ &&
           utf16_index_ == rhs.utf16_index_ && utf32_index_ == rhs.utf32_index_;
  }

  std::string DebugString() const {
    return IcingStringUtil::StringPrintf("(u8:%d,u16:%d,u32:%d)", utf8_index_,
                                         utf16_index_, utf32_index_);
  }

 private:
  // Resets the character iterator to the start of the text if any of the
  // indices are negative.
  void ResetToStartIfNecessary();

  std::string_view text_;
  mutable UChar32 cached_current_char_;
  int utf8_index_;
  int utf16_index_;
  int utf32_index_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_CHARACTER_ITERATOR_H_
