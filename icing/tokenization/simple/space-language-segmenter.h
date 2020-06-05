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

#ifndef ICING_TOKENIZATION_SIMPLE_SPACE_LANGUAGE_SEGMENTER_H_
#define ICING_TOKENIZATION_SIMPLE_SPACE_LANGUAGE_SEGMENTER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/tokenization/language-segmenter.h"

namespace icing {
namespace lib {

// Simple segmenter that splits on spaces, regardless of language. Continuous
// whitespaces will be returned as a single whitespace character.
class SpaceLanguageSegmenter : public LanguageSegmenter {
 public:
  SpaceLanguageSegmenter() = default;
  SpaceLanguageSegmenter(const SpaceLanguageSegmenter&) = delete;
  SpaceLanguageSegmenter& operator=(const SpaceLanguageSegmenter&) = delete;

  // Segmentation is based purely on whitespace; does not take into account the
  // language of the text.
  //
  // Returns:
  //   An iterator of terms on success
  libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter::Iterator>>
  Segment(std::string_view text) const override;

  // Does not take into account the language of the text.
  //
  // Returns:
  //   A list of terms on success
  //   INTERNAL_ERROR if any error occurs
  libtextclassifier3::StatusOr<std::vector<std::string_view>> GetAllTerms(
      std::string_view text) const override;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_TOKENIZATION_SIMPLE_SPACE_LANGUAGE_SEGMENTER_H_
