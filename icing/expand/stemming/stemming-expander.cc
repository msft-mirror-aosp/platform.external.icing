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

#include "icing/expand/stemming/stemming-expander.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/mutex.h"
#include "icing/expand/expander.h"
#include "icing/expand/stemming/stemmer-factory.h"
#include "icing/expand/stemming/stemmer.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

/*static*/ libtextclassifier3::StatusOr<std::unique_ptr<StemmingExpander>>
StemmingExpander::Create(std::string language_code) {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Stemmer> stemmer,
                         stemmer_factory::Create(language_code));

  return std::unique_ptr<StemmingExpander>(
      new StemmingExpander(std::move(language_code), std::move(stemmer)));
}

std::vector<ExpandedTerm> StemmingExpander::Expand(
    std::string_view term) const {
  std::vector<ExpandedTerm> result;
  // Add the original term.
  result.emplace_back(std::string(term), /*is_stemmed_term=*/false);

  libtextclassifier3::StatusOr<std::unique_ptr<Stemmer>> stemmer_or =
      ProduceStemmer();
  if (!stemmer_or.ok()) {
    return result;
  }

  std::unique_ptr<Stemmer> stemmer = std::move(stemmer_or).ValueOrDie();
  std::string stemmed_term = stemmer->Stem(term);
  ReturnStemmer(std::move(stemmer));

  if (stemmed_term != term) {
    result.emplace_back(std::move(stemmed_term), /*is_stemmed_term=*/true);
  }
  return result;
}

libtextclassifier3::StatusOr<std::unique_ptr<Stemmer>>
StemmingExpander::ProduceStemmer() const {
  std::unique_ptr<Stemmer> stemmer = nullptr;
  {
    absl_ports::unique_lock l(&mutex_);
    if (cached_stemmer_ != nullptr) {
      stemmer = std::move(cached_stemmer_);
    }
  }
  if (stemmer == nullptr) {
    ICING_ASSIGN_OR_RETURN(stemmer, stemmer_factory::Create(language_code_));
  }

  return stemmer;
}

void StemmingExpander::ReturnStemmer(std::unique_ptr<Stemmer> stemmer) const {
  absl_ports::unique_lock l(&mutex_);
  if (!cached_stemmer_) {
    cached_stemmer_ = std::move(stemmer);
  }
}

}  // namespace lib
}  // namespace icing
