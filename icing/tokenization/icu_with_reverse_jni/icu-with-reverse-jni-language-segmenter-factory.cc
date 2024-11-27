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

#ifndef ICING_TOKENIZATION_ICU_WITH_REVERSE_JNI_ICU_WITH_REVERSE_JNI_LANGUAGE_SEGMENTER_FACTORY_H_
#define ICING_TOKENIZATION_ICU_WITH_REVERSE_JNI_ICU_WITH_REVERSE_JNI_LANGUAGE_SEGMENTER_FACTORY_H_

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/tokenization/language-segmenter-factory.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "icing/absl_ports/canonical_errors.h"
#include "icing/tokenization/icu/icu-language-segmenter.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/tokenization/reverse_jni/reverse-jni-language-segmenter.h"
#include "icing/util/logging.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace language_segmenter_factory {

constexpr std::string_view kLocaleAmericanEnglishComputer = "en_US_POSIX";

// Creates a language segmenter based on the enable_icu_segmenter
// flag in SegmenterOptions:
//   - If enable_icu_segmenter is true, creates an ICU based segmenter.
//   - If enable_icu_segmenter is false, creates a Reverse JNI based segmenter.
//
// Returns:
//   A LanguageSegmenter on success
//   INVALID_ARGUMENT_ERROR if locale string is invalid or jni_cache is nullptr
//                    when enable_icu_segmenter is false
//
// TODO(b/156383798): Figure out if we want to verify locale strings and notify
// users. Right now illegal locale strings will be ignored by ICU. ICU
// components will be created with its default locale.
libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter>> Create(
    SegmenterOptions options) {
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

  if (options.enable_icu_segmenter) {
    return IcuLanguageSegmenter::Create(std::move(options.locale));
  } else {
    if (options.jni_cache == nullptr) {
      return absl_ports::InvalidArgumentError(
          "Cannot create Reverse Jni Language Segmenter without a valid "
          "JniCache pointer");
    }
    return std::make_unique<ReverseJniLanguageSegmenter>(
        std::move(options.locale), options.jni_cache);
  }
}

}  // namespace language_segmenter_factory

}  // namespace lib
}  // namespace icing

#endif  // ICING_TOKENIZATION_ICU_WITH_REVERSE_JNI_ICU_WITH_REVERSE_JNI_LANGUAGE_SEGMENTER_FACTORY_H_
