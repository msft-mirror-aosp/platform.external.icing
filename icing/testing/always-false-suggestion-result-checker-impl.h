// Copyright (C) 2021 Google LLC
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

#ifndef THIRD_PARTY_ICING_TESTING_ALWAYS_TRUE_SUGGESTION_RESULT_CHECKER_IMPL_H_
#define THIRD_PARTY_ICING_TESTING_ALWAYS_TRUE_SUGGESTION_RESULT_CHECKER_IMPL_H_

#include "third_party/icing/schema/section.h"
#include "third_party/icing/store/document-id.h"
#include "third_party/icing/store/suggestion-result-checker.h"

namespace icing {
namespace lib {

class AlwaysFalseSuggestionResultCheckerImpl : public SuggestionResultChecker {
 public:
  bool BelongsToTargetResults(DocumentId document_id,
                              SectionId section_id) const override {
    return false;
  }
};

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_TESTING_ALWAYS_TRUE_SUGGESTION_RESULT_CHECKER_IMPL_H_