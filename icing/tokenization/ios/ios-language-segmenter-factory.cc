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

#include "icing/tokenization/ios/ios-language-segmenter.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

namespace language_segmenter_factory {

namespace {
constexpr std::string_view kLocaleAmericanEnglishComputer = "en_US_POSIX";
}  // namespace

// Creates a language segmenter with the given locale.
//
// Returns:
//   A LanguageSegmenter on success
//   INVALID_ARGUMENT if locale string is invalid
libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter>> Create(
    SegmenterOptions options) {
  // Word connector rules for "en_US_POSIX" (American English (Computer)) are
  // different from other locales. E.g. "email.subject" will be split into 3
  // terms in "en_US_POSIX": "email", ".", and "subject", while it's just one
  // term in other locales. Our current LanguageSegmenter doesn't handle this
  // special rule, so we replace it with "en_US".
  if (options.locale == kLocaleAmericanEnglishComputer) {
    ICING_LOG(WARNING) << "Locale " << kLocaleAmericanEnglishComputer
                       << " not supported. Converting to locale en_US";
    options.locale = "en_US";
  }
  return std::make_unique<IosLanguageSegmenter>(std::move(options.locale));
}

}  // namespace language_segmenter_factory

}  // namespace lib
}  // namespace icing
