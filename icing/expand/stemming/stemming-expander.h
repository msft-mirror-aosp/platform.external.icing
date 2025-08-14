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

#ifndef ICING_EXPAND_STEMMING_STEMMING_EXPANDER_H_
#define ICING_EXPAND_STEMMING_STEMMING_EXPANDER_H_

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/thread_annotations.h"
#include "icing/expand/expander.h"
#include "icing/expand/stemming/stemmer.h"

namespace icing {
namespace lib {

// Used to expand a given term to its root form.
//
// This class is thread-safe.
class StemmingExpander : public Expander {
 public:
  static libtextclassifier3::StatusOr<std::unique_ptr<StemmingExpander>> Create(
      std::string language_code);

  ~StemmingExpander() override {}

  // Expands the given term to its root form.
  //
  // The expanded vector will contain either:
  //   - A single element containing the original term, if the stemmer does not
  //     produce a different term, or
  //   - Two elements (the original term and its stem), with the first element
  //     being the original term.
  std::vector<ExpandedTerm> Expand(std::string_view term) const override;

 private:
  explicit StemmingExpander(std::string language_code,
                            std::unique_ptr<Stemmer> stemmer)
      : language_code_(std::move(language_code)),
        cached_stemmer_(std::move(stemmer)) {}

  // Produces a stemmer in the language of the StemmingExpander that the caller
  // owns.
  //  - If cached_stemmer_ is not null, transfers ownership to the caller and
  //    sets cached_stemmer_ to null.
  //  - Otherwise, creates a new stemmer and transfers ownership to the caller.
  //
  // Note: Caller must call ReturnStemmer() after using the stemmer.
  //
  // Returns:
  //  - A stemmer for a given language on success.‰
  //  - INVALID_ARGUMENT_ERROR if the language code is invalid or not supported.
  //  - INTERNAL_ERROR on errors.
  //
  // Requires:
  //  - language_code_ is a valid code for the stemmer.
  libtextclassifier3::StatusOr<std::unique_ptr<Stemmer>> ProduceStemmer() const
      ICING_LOCKS_EXCLUDED(mutex_);

  // Caller transfers ownership of stemmer to the StemmingExpander.
  //  - If cached_stemmer_ is not null, stemmer will be deleted.
  //  - Otherwise, the stemmer becomes cached_stemmer_.
  void ReturnStemmer(std::unique_ptr<Stemmer>) const
      ICING_LOCKS_EXCLUDED(mutex_);

  // The language code of the stemmer.
  const std::string language_code_;

  // A cached stemmer that is used to expand a term.
  //
  // The stemmer is not thread-safe.
  mutable std::unique_ptr<Stemmer> cached_stemmer_ ICING_GUARDED_BY(mutex_);

  // Used to provide reader and writer locks
  mutable absl_ports::shared_mutex mutex_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_EXPAND_STEMMING_STEMMING_EXPANDER_H_
