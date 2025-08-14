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

#ifndef ICING_EXPAND_EXPANDER_MANAGER_H_
#define ICING_EXPAND_EXPANDER_MANAGER_H_

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/thread_annotations.h"
#include "icing/expand/expander.h"
#include "icing/proto/term.pb.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

// This class is a wrapper around the various expanders. It is responsible for
// calling the appropriate expander based on a term's match type and locale.
//
// This class is thread-safe.
class ExpanderManager {
 public:
  // This is used as the default locale if the provided locale is invalid.
  static constexpr std::string_view kDefaultEnglishLocale = ULOC_US;

  // Map of a locale to an expander.
  //
  // The Expander instances are managed exclusively by this class, and are never
  // deleted from the map once created. Therefore we don't need std::shared_ptr
  // here even though multiple threads may be accessing the same expander
  // instance at the same time.
  using ExpandersMap =
      std::unordered_map<std::string, std::unique_ptr<Expander>>;

  // Factory method to create an ExpanderManager. The expanders will be
  // initialized in the default locale.
  //
  // Returns:
  //  - An ExpanderManager on success.
  //  - INVALID_ARGUMENT_ERROR if max_terms_per_expander <= 1.
  //  - INTERNAL_ERROR ion errors.
  static libtextclassifier3::StatusOr<std::unique_ptr<ExpanderManager>> Create(
      std::string default_locale, int max_terms_per_expander);

  // Processes a term according to the term's match type and locale. The
  // first ExpandedTerm in the returned list will always be the original input
  // term.
  //
  // A new expander will be created when possible if the expander corresponding
  // to the given term match type and locale does not already exist.
  // If the locale is not supported, the term will be expanded using the default
  // locale.
  //
  // Returns: a list of expanded terms.
  std::vector<ExpandedTerm> ProcessTerm(std::string_view term,
                                        TermMatchType::Code term_match_type,
                                        const std::string& locale);

  const std::string& default_locale() const { return default_locale_; }

 private:
  explicit ExpanderManager(ExpandersMap stemming_expanders,
                           std::string default_locale,
                           int max_terms_per_expander)
      : stemming_expanders_(std::move(stemming_expanders)),
        default_locale_(std::move(default_locale)),
        max_terms_per_expander_(max_terms_per_expander) {}

  // Returns a stemming expander for the given locale.
  // - Returns the expander retrieved from the stemming_expanders_ map if an
  //   instance already exists for the locale.
  // - Otherwise, creates a new expander instance and adds it to the
  //   stemming_expanders_ map before returning it.
  const Expander& GetOrCreateStemmingExpander(const std::string& locale)
      ICING_LOCKS_EXCLUDED(mutex_);

  // Map of locale to stemming expanders.
  ExpandersMap stemming_expanders_ ICING_GUARDED_BY(mutex_);

  // Default locale to use for expanders.
  const std::string default_locale_;

  // Maximum number of terms to expand to for an input term per expander. This
  // number includes the input term.
  const int max_terms_per_expander_;

  // Used to provide reader and writer locks
  mutable absl_ports::shared_mutex mutex_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_EXPAND_EXPANDER_MANAGER_H_
