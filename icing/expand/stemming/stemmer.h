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

#ifndef ICING_EXPAND_STEMMING_STEMMER_H_
#define ICING_EXPAND_STEMMING_STEMMER_H_

#include <string>
#include <string_view>

namespace icing {
namespace lib {

// Stems a given term to its root form.
//
// Example usage:
//   std::unique_ptr<Stemmer> stemmer = stemmer_factory::Create("en");
//   std::string stem = stemmer->Stem("running");
class Stemmer {
 public:
  virtual ~Stemmer() = default;

  // Returns the stem of a given term.
  // This returns the same value if the term is already in its stem form.
  virtual std::string Stem(std::string_view term) const = 0;

  // Returns the language code of the stemmer.
  virtual const std::string& language_code() const = 0;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_EXPAND_STEMMING_STEMMER_H_
