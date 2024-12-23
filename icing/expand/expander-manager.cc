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

#include "icing/expand/expander-manager.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/mutex.h"
#include "icing/expand/expander.h"
#include "icing/expand/stemming/stemming-expander.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

const Expander& ExpanderManager::GetOrCreateStemmingExpander(
    const std::string& locale) {
  {
    // Check if the expander already exists. This only requires a read lock.
    absl_ports::shared_lock l(&mutex_);
    auto itr = stemming_expanders_.find(locale);
    if (itr != stemming_expanders_.end()) {
      return *(itr->second);
    }
  }

  const char* stemmer_language_code = uloc_getISO3Language(locale.c_str());
  libtextclassifier3::StatusOr<std::unique_ptr<StemmingExpander>> expander_or =
      StemmingExpander::Create(stemmer_language_code);

  if (!expander_or.status().ok()) {
    ICING_VLOG(1) << "Failed to create stemming expander for locale: " << locale
                  << ". Using default locale: " << default_locale_;
    {
      absl_ports::shared_lock l(&mutex_);
      // stemming_expanders_[default_locale_] is guaranteed to exist as this is
      // created during initialization.
      return *stemming_expanders_[default_locale_];
    }
  }

  std::unique_ptr<Expander> stemming_expander =
      std::move(expander_or).ValueOrDie();
  {
    absl_ports::unique_lock l(&mutex_);
    // Check again before emplacing into the map in case the expander was
    // created by another thread.
    auto itr = stemming_expanders_.find(locale);
    if (itr == stemming_expanders_.end()) {
      itr = stemming_expanders_.emplace(locale, std::move(stemming_expander))
                .first;
    }
    return *(itr->second);
  }
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<ExpanderManager>>
ExpanderManager::Create(std::string default_locale,
                        int max_terms_per_expander) {
  if (max_terms_per_expander <= 1) {
    return libtextclassifier3::Status(
        libtextclassifier3::StatusCode::INVALID_ARGUMENT,
        "max_num_expanded_terms must be greater than 1.");
  }

  // Create a default stemming expander using defalt_locale. This is added into
  // the stemming_expanders_ map during initialization.
  const char* stemmer_language_code =
      uloc_getISO3Language(default_locale.c_str());
  libtextclassifier3::StatusOr<std::unique_ptr<StemmingExpander>> expander_or =
      StemmingExpander::Create(stemmer_language_code);

  std::unique_ptr<StemmingExpander> expander;
  if (!expander_or.status().ok()) {
    ICING_VLOG(1) << "Failed to create expander manager with locale: "
                  << default_locale
                  << ". Using default English locale instead.";
    default_locale = kDefaultEnglishLocale;
    stemmer_language_code = uloc_getISO3Language(default_locale.c_str());
    ICING_ASSIGN_OR_RETURN(expander,
                           StemmingExpander::Create(stemmer_language_code));
  } else {
    expander = std::move(expander_or).ValueOrDie();
  }

  ExpandersMap stemming_expanders;
  stemming_expanders.emplace(default_locale, std::move(expander));
  return std::unique_ptr<ExpanderManager>(
      new ExpanderManager(std::move(stemming_expanders),
                          std::move(default_locale), max_terms_per_expander));
}

std::vector<ExpandedTerm> ExpanderManager::ProcessTerm(
    std::string_view term, TermMatchType::Code term_match_type,
    const std::string& locale) {
  switch (term_match_type) {
    case TermMatchType_Code_UNKNOWN:
    case TermMatchType::EXACT_ONLY:
    case TermMatchType::PREFIX: {
      // Return the original term.
      std::vector<ExpandedTerm> expanded_terms;
      expanded_terms.push_back(
          ExpandedTerm(std::string(term), /*is_stemmed_term=*/false));
      return expanded_terms;
    }
    case TermMatchType_Code_STEMMING: {
      // The stemming expander returns at most 2 terms, and we don't allow
      // having max_terms_per_expander < 2, so we don't need to check the size
      // of the returned vector here.
      return GetOrCreateStemmingExpander(locale).Expand(term);
    }
  }
}

}  // namespace lib
}  // namespace icing
