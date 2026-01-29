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

#ifndef ICING_EXPAND_STEMMING_SIMPLE_NONE_STEMMER_H_
#define ICING_EXPAND_STEMMING_SIMPLE_NONE_STEMMER_H_

#include <string>
#include <string_view>

#include "icing/expand/stemming/stemmer.h"

namespace icing {
namespace lib {

// A dummy stemmer that just returns the input term.
//
// This is useful for importing and building in environments where we cannot
// have external dependencies for stemming (e.g. Jetpack).
//
// Example usage:
//   auto stemmer = stemmer_factory::Create("en");
//   stemmer->Stem("running");    // Returns "running"
class NoneStemmer : public Stemmer {
 public:
  static constexpr std::string_view kNoneStemmerLanguageCode = "none";

  explicit NoneStemmer() : language_code_(kNoneStemmerLanguageCode) {};

  std::string Stem(std::string_view term) const override {
    return std::string(term);
  }

  const std::string& language_code() const override { return language_code_; }

 private:
  const std::string language_code_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_EXPAND_STEMMING_SIMPLE_NONE_STEMMER_H_
