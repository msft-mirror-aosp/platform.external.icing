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

#include <utility>

#include "icing/tokenization/icu/icu-language-segmenter.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/util/logging.h"
#include "icing/util/status-util.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace language_segmenter_factory {

using ::icing::lib::status_util::TransformStatus;

namespace {
constexpr std::string_view kLocaleAmericanEnglishComputer = "en_US_POSIX";
}  // namespace

// Creates a language segmenter based on the provided options.
//
// @param options: The options for creating the language segmenter.
// @param icu_segmenter_creation_status: Optional output parameter that will be
//        populated with the status of IcuLanguageSegmenter.
//
// Returns:
//   A LanguageSegmenter on success
//   INVALID_ARGUMENT_ERROR if locale string is invalid
//
// TODO(b/156383798): Figure out if we want to verify locale strings and notify
// users. Right now illegal locale strings will be ignored by ICU. ICU
// components will be created with its default locale.
libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter>> Create(
    SegmenterOptions options, StatusProto* icu_segmenter_creation_status) {
  // Word connector rules for "en_US_POSIX" (American English (Computer)) are
  // different from other locales. E.g. "email.subject" will be split into 3
  // terms in "en_US_POSIX": "email", ".", and "subject", while it's just one
  // term in other locales. Our current LanguageSegmenter doesn't handle this
  // special rule, so we replace it with "en_US".
  if (options.locale == kLocaleAmericanEnglishComputer) {
    ICING_LOG(WARNING) << "Locale " << kLocaleAmericanEnglishComputer
                       << " not supported. Converting to locale " << ULOC_US;
    options.locale = ULOC_US;
  }
  auto icu_segmenter_or =
      IcuLanguageSegmenter::Create(std::move(options.locale));
  if (icu_segmenter_creation_status != nullptr) {
    TransformStatus(icu_segmenter_or.status(), icu_segmenter_creation_status);
  }
  return icu_segmenter_or;
}

}  // namespace language_segmenter_factory

}  // namespace lib
}  // namespace icing
