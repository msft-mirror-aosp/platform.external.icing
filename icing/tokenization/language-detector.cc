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

#include "icing/tokenization/language-detector.h"

#include "utils/base/statusor.h"
#include "nlp/saft/components/lang_id/mobile/fb_model/lang-id-from-fb.h"
#include "nlp/saft/components/lang_id/mobile/lang-id.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"

namespace icing {
namespace lib {
using ::nlp_saft::mobile::lang_id::GetLangIdFromFlatbufferFile;
using ::nlp_saft::mobile::lang_id::LangId;

class LanguageDetectorWithLangId : public LanguageDetector {
 public:
  static libtextclassifier3::StatusOr<
      std::unique_ptr<LanguageDetectorWithLangId>>
  Create(const std::string& lang_id_model_path) {
    auto language_detector = std::unique_ptr<LanguageDetectorWithLangId>(
        new LanguageDetectorWithLangId(lang_id_model_path));
    if (language_detector->is_valid()) {
      return language_detector;
    }
    return absl_ports::InvalidArgumentError(absl_ports::StrCat(
        "Failed to create a language detector with LangId model path: ",
        lang_id_model_path));
  }

  libtextclassifier3::StatusOr<std::string> DetectLanguage(
      std::string_view text) const override {
    const std::string& lang_found =
        lang_id_->FindLanguage(text.data(), text.length());
    if (lang_found == LangId::kUnknownLanguageCode) {
      return absl_ports::NotFoundError(
          absl_ports::StrCat("Language not found in text: ", text));
    }
    return lang_found;
  }

 private:
  // TODO(samzheng): Use GetLangIdWithParamsFromCc() as a fallback when it's
  // available in AOSP
  explicit LanguageDetectorWithLangId(const std::string& lang_id_model_path)
      : lang_id_(GetLangIdFromFlatbufferFile(lang_id_model_path)) {}

  std::unique_ptr<LangId> lang_id_;

  bool is_valid() { return lang_id_->is_valid(); }
};

libtextclassifier3::StatusOr<std::unique_ptr<LanguageDetector>>
LanguageDetector::CreateWithLangId(const std::string& lang_id_model_path) {
  return LanguageDetectorWithLangId::Create(lang_id_model_path);
}

}  // namespace lib
}  // namespace icing
