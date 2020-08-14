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

#ifndef ICING_TOKENIZATION_LANGUAGE_SEGMENTER_H_
#define ICING_TOKENIZATION_LANGUAGE_SEGMENTER_H_

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"

namespace icing {
namespace lib {

// A base class that all other LanguageSegmenters should inherit from. It
// provides interfaces that allow callers to segment text. The return value
// could be an iterator or a list of tokens. Example usage:
//
// std::unique_ptr<LanguageSegmenter> segmenter = GetSegmenter();
// ICING_ASSIGN_OR_RETURN(std::unique_ptr<LanguageSegmenter::Iterator> iter,
//                  segmenter->Segment(text));
// ICING_ASSIGN_OR_RETURN(std::vector<std::string_view> segments,
// segmenter->GetAllTerms(text));
class LanguageSegmenter {
 public:
  virtual ~LanguageSegmenter() = default;

  // An iterator helping to find terms in the input text.
  // Example usage:
  //
  // while (iterator.Advance()) {
  //   const std::string_view term = iterator.GetTerm();
  //   // Do something
  // }
  class Iterator {
   public:
    virtual ~Iterator() = default;

    // Advances to the next term. Returns false if it has reached the end.
    virtual bool Advance() = 0;

    // Returns the current term. It can be called only when Advance() returns
    // true.
    virtual std::string_view GetTerm() const = 0;

    // Resets the iterator to point to the first term that starts after offset.
    // GetTerm will now return that term. For example:
    //
    //   language_segmenter = language_segmenter_factory::Create(type);
    //   iterator = language_segmenter->Segment("foo bar baz");
    //   iterator.ResetToTermStartingAfter(4);
    //   iterator.GetTerm() // returns "baz";
    //
    // Return types of OK and NOT_FOUND indicate that the function call was
    // valid and the state of the iterator has changed. Return type of
    // INVALID_ARGUMENT will leave the iterator unchanged.
    //
    // Returns:
    //   On success, the starting position of the first term that starts after
    //   offset.
    //   NOT_FOUND if an error occurred or there are no terms that start after
    //   offset.
    //   INVALID_ARGUMENT if offset is out of bounds for the provided text.
    //   ABORTED if an invalid unicode character is encountered while
    //   traversing the text.
    virtual libtextclassifier3::StatusOr<int32_t> ResetToTermStartingAfter(
        int32_t offset) = 0;

    // Resets the iterator to point to the first term that ends before offset.
    // GetTerm will now return that term. For example:
    //
    //   language_segmenter = language_segmenter_factory::Create(type);
    //   iterator = language_segmenter->Segment("foo bar baz");
    //   iterator.ResetToTermEndingBefore(7);
    //   iterator.GetTerm() // returns "bar";
    //
    // Return types of OK and NOT_FOUND indicate that the function call was
    // valid and the state of the iterator has changed. Return type of
    // INVALID_ARGUMENT will leave the iterator unchanged.
    //
    // Returns:
    //   On success, the starting position of the first term that ends before
    //   offset.
    //   NOT_FOUND if an error occurred or there are no terms that ends before
    //   offset.
    //   INVALID_ARGUMENT if offset is out of bounds for the provided text.
    //   ABORTED if an invalid unicode character is encountered while
    //   traversing the text.
    virtual libtextclassifier3::StatusOr<int32_t> ResetToTermEndingBefore(
        int32_t offset) = 0;

    virtual libtextclassifier3::StatusOr<int32_t> ResetToStart() = 0;
  };

  // Segments the input text into terms.
  //
  // Returns:
  //   An iterator of terms on success
  //   INTERNAL_ERROR if any error occurs
  //
  // Note: The underlying char* data of the input string won't be copied but
  // shared with the return strings, so please make sure the input string
  // outlives the returned iterator.
  virtual libtextclassifier3::StatusOr<
      std::unique_ptr<LanguageSegmenter::Iterator>>
  Segment(std::string_view text) const = 0;

  // Segments and returns all terms in the input text.
  //
  // Returns:
  //   A list of terms on success
  //   INTERNAL_ERROR if any error occurs
  //
  // Note: The underlying char* data of the input string won't be copied but
  // shared with the return strings, so please make sure the input string
  // outlives the returned terms.
  virtual libtextclassifier3::StatusOr<std::vector<std::string_view>>
  GetAllTerms(std::string_view text) const = 0;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_TOKENIZATION_LANGUAGE_SEGMENTER_H_
