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

#ifndef ICING_QUERY_QUERY_PROCESSOR_H_
#define ICING_QUERY_QUERY_PROCESSOR_H_

#include <memory>

#include "utils/base/statusor.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/proto/search.pb.h"
#include "icing/query/query-terms.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-store.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {

// Processes SearchSpecProtos and retrieves the specified DocHitInfos that
// satisfies the query and its restrictions. This does not perform any scoring,
// and returns matched documents in a descending DocumentId order.
class QueryProcessor {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the one constructed.
  // TODO(b/141180665): Add nullptr checks for the raw pointers
  explicit QueryProcessor(Index* index,
                          const LanguageSegmenter* language_segmenter,
                          const Normalizer* normalizer,
                          const DocumentStore* document_store,
                          const SchemaStore* schema_store, const Clock* clock);

  struct QueryResults {
    std::unique_ptr<DocHitInfoIterator> root_iterator;
    // A map from section names to sets of terms restricted to those sections.
    // Query terms that are not restricted are found at the entry with key "".
    SectionRestrictQueryTermsMap query_terms;
  };
  // Parse the search configurations (including the query, any additional
  // filters, etc.) in the SearchSpecProto into one DocHitInfoIterator.
  //
  // Returns:
  //   On success,
  //     - One iterator that represents the entire query
  //     - A map representing the query terms and any section restrictions
  //   INVALID_ARGUMENT if query syntax is incorrect and cannot be tokenized
  //   INTERNAL_ERROR on all other errors
  libtextclassifier3::StatusOr<QueryResults> ParseSearch(
      const SearchSpecProto& search_spec);

 private:
  // Parse the query into a one DocHitInfoIterator that represents the root of a
  // query tree.
  //
  // Returns:
  //   On success,
  //     - One iterator that represents the entire query
  //     - A map representing the query terms and any section restrictions
  //   INVALID_ARGUMENT if query syntax is incorrect and cannot be tokenized
  //   INTERNAL_ERROR on all other errors
  libtextclassifier3::StatusOr<QueryResults> ParseRawQuery(
      const SearchSpecProto& search_spec);

  // Not const because we could modify/sort the hit buffer in the lite index at
  // query time.
  Index& index_;
  const LanguageSegmenter& language_segmenter_;
  const Normalizer& normalizer_;
  const DocumentStore& document_store_;
  const SchemaStore& schema_store_;
  const Clock& clock_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_QUERY_QUERY_PROCESSOR_H_
