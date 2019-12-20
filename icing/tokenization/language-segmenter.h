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
#include <string>
#include <string_view>
#include <vector>

#include "utils/base/status.h"
#include "utils/base/statusor.h"
#include "icing/tokenization/language-detector.h"
#include "unicode/ubrk.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

// This class is used to segment sentences into words based on rules
// (https://unicode.org/reports/tr29/#Word_Boundaries) and language
// understanding. Based on the basic segmentation done by UBreakIterator,
// some extra rules are applied in this class:
//
// 1. All ASCII terms will be returned.
// 2. For non-ASCII terms, only the alphabetic terms are returned, which means
//    non-ASCII punctuation and special characters are left out.
// 3. Multiple continuous whitespaces are treated as one.
//
// The rules above are common to the high-level tokenizers that might use this
// class. Other special tokenization logic will be in each tokenizer.
class LanguageSegmenter {
 public:
  LanguageSegmenter(const LanguageSegmenter&) = delete;
  LanguageSegmenter& operator=(const LanguageSegmenter&) = delete;

  // Creates a language segmenter that uses the given LangId model. Default
  // locale is used when language can't be detected.
  //
  // Returns:
  //   A LanguageSegmenter on success
  //   INVALID_ARGUMENT if fails to load model
  static libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter>>
  Create(const std::string& lang_id_model_path,
         const std::string& default_locale = ULOC_US);

  // An iterator helping to find terms in the input text.
  // Example usage:
  //
  // while (iterator.Advance()) {
  //   const std::string_view term = iterator.GetTerm();
  //   // Do something
  // }
  class Iterator {
   public:
    // Factory function to create a segment iterator based on the given locale.
    //
    // Returns:
    //   An iterator on success
    //   INTERNAL_ERROR if unable to create
    static libtextclassifier3::StatusOr<
        std::unique_ptr<LanguageSegmenter::Iterator>>
    Create(std::string_view text, const std::string locale);

    ~Iterator();

    // Advances to the next term. Returns false if it has reached the end.
    bool Advance();

    // Returns the current term. It can be called only when Advance() returns
    // true.
    std::string_view GetTerm() const;

    // Resets the iterator to point to the first term that starts after offset.
    // GetTerm will now return that term.
    //
    // Returns:
    //   On success, the starting position of the first term that starts after
    //   offset.
    //   NOT_FOUND if an error occurred or there are no terms that start after
    //   offset.
    libtextclassifier3::StatusOr<int32_t> ResetToTermStartingAfter(
        int32_t offset);

    // Resets the iterator to point to the first term that ends before offset.
    // GetTerm will now return that term.
    //
    // Returns:
    //   On success, the starting position of the first term that ends before
    //   offset.
    //   NOT_FOUND if an error occurred or there are no terms that ends before
    //   offset.
    libtextclassifier3::StatusOr<int32_t> ResetToTermEndingBefore(
        int32_t offset);

   private:
    Iterator(std::string_view text, const std::string&& locale);

    // Returns true on success
    bool Initialize();

    // Resets the iterator to point to the first term that starts before offset.
    // GetTerm will now return that term.
    //
    // Returns:
    //   OK on success
    //   NOT_FOUND if an error occurred or there are no terms that start before
    //   offset.
    libtextclassifier3::Status ResetToTermStartingBefore(int32_t offset);

    // The underlying class that does the segmentation, ubrk_close() must be
    // called after using.
    UBreakIterator* break_iterator_;

    // Text to be segmented
    const std::string_view text_;

    // Locale of the input text, used to help segment more accurately. If a
    // wrong locale is set, text could probably still be segmented correctly
    // because the default break iterator behavior is used for most locales.
    const std::string locale_;

    // A thin wrapper around the input UTF8 text, needed by break_iterator_.
    // utext_close() must be called after using.
    UText u_text_;

    // The start and end indices are used to track the positions of current
    // term.
    int term_start_index_;
    int term_end_index_exclusive_;
  };

  // Segments the input text into terms. The segmentation depends on the
  // language detected in the input text.
  //
  // Returns:
  //   An iterator of terms on success
  //   INTERNAL_ERROR if any error occurs
  //
  // Note: The underlying char* data of the input string won't be copied but
  // shared with the return strings, so please make sure the input string
  // outlives the returned iterator.
  //
  // Note: It could happen that the language detected from text is wrong, then
  // there would be a small chance that the text is segmented incorrectly.
  libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter::Iterator>>
  Segment(std::string_view text) const;

  // Segments and returns all terms in the input text. The segmentation depends
  // on the language detected in the input text.
  //
  // Returns:
  //   A list of terms on success
  //   INTERNAL_ERROR if any error occurs
  //
  // Note: The underlying char* data of the input string won't be copied but
  // shared with the return strings, so please make sure the input string
  // outlives the returned terms.
  //
  // Note: It could happen that the language detected from text is wrong, then
  // there would be a small chance that the text is segmented incorrectly.
  libtextclassifier3::StatusOr<std::vector<std::string_view>> GetAllTerms(
      std::string_view text) const;

 private:
  LanguageSegmenter(std::unique_ptr<LanguageDetector> language_detector,
                    const std::string default_locale);

  // Used to detect languages in text
  const std::unique_ptr<LanguageDetector> language_detector_;

  // Used as default locale when language can't be detected in text
  const std::string default_locale_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_TOKENIZATION_LANGUAGE_SEGMENTER_H_
