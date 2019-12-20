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

#ifndef ICING_TOKENIZATION_LANGUAGE_DETECTOR_H_
#define ICING_TOKENIZATION_LANGUAGE_DETECTOR_H_

#include <memory>
#include <string>
#include <string_view>

#include "utils/base/statusor.h"

namespace icing {
namespace lib {

class LanguageDetector {
 public:
  virtual ~LanguageDetector() = default;

  // Creates a language detector that uses the given LangId model.
  //
  // Returns:
  //   A LanguageDetector on success
  //   INVALID_ARGUMENT if fails to load model
  static libtextclassifier3::StatusOr<std::unique_ptr<LanguageDetector>>
  CreateWithLangId(const std::string& lang_id_model_path);

  // Detects the language of the given text, if there're multiple languages, the
  // one with the biggest possibility will be returned. The two-letter language
  // code uses the ISO-639 standard (https://en.wikipedia.org/wiki/ISO_639).
  //
  // Returns:
  //   language code on success
  //   NOT_FOUND if no language detected
  virtual libtextclassifier3::StatusOr<std::string> DetectLanguage(
      std::string_view text) const = 0;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_TOKENIZATION_LANGUAGE_DETECTOR_H_
