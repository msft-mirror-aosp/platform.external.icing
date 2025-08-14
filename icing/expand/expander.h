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

#ifndef ICING_EXPAND_EXPANDER_H_
#define ICING_EXPAND_EXPANDER_H_

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace icing {
namespace lib {

// Struct to hold the text of a term, and whether it has undergone certain
// expansion operations.
struct ExpandedTerm {
  std::string text;
  bool is_stemmed_term;

  explicit ExpandedTerm(std::string text_in, bool is_stemmed_term_in)
      : text(std::move(text_in)), is_stemmed_term(is_stemmed_term_in) {}

  bool operator==(const ExpandedTerm& other) const {
    return text == other.text && is_stemmed_term == other.is_stemmed_term;
  }
};

class Expander {
 public:
  virtual ~Expander() = default;

  // Expands a given term into a vector of expanded terms. The first term in the
  // output vector is always the original term.
  //
  // See implementation classes for specific expansion behaviors.
  virtual std::vector<ExpandedTerm> Expand(std::string_view term) const = 0;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_EXPAND_EXPANDER_H_
