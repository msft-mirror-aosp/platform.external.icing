// Copyright (C) 2022 Google LLC
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

#ifndef ICING_QUERY_ADVANCED_QUERY_PROCESSOR_H_
#define ICING_QUERY_ADVANCED_QUERY_PROCESSOR_H_

#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/proto/search.pb.h"
#include "icing/query/list_filter/syntax.pb.h"
#include "icing/query/query-results.h"
#include "icing/query/query-terms.h"
#include "icing/query/query-utils.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-store.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer.h"

namespace icing {
namespace lib {

// Processes SearchSpecProtos and retrieves the specified DocHitInfos that
// satisfies the query and its restrictions. This does not perform any scoring,
// and returns matched documents in a descending DocumentId order.
class AdvancedQueryProcessor {
 public:
  // Factory function to create a QueryProcessor which does not take ownership
  // of any input components, and all pointers must refer to valid objects that
  // outlive the created QueryProcessor instance.
  //
  // Returns:
  //   An QueryProcessor on success
  //   FAILED_PRECONDITION if any of the pointers is null.
  static libtextclassifier3::StatusOr<std::unique_ptr<AdvancedQueryProcessor>>
  Create(Index* index, const LanguageSegmenter* language_segmenter,
         const Normalizer* normalizer, const DocumentStore* document_store,
         const SchemaStore* schema_store);

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
      const SearchSpecProto& search_spec,
      ScoringSpecProto::RankingStrategy::Code ranking_strategy);

 private:
  explicit AdvancedQueryProcessor(Index* index,
                                  const LanguageSegmenter* language_segmenter,
                                  const Normalizer* normalizer,
                                  const DocumentStore* document_store,
                                  const SchemaStore* schema_store)
      : index_(*index),
        language_segmenter_(*language_segmenter),
        normalizer_(*normalizer),
        document_store_(*document_store),
        schema_store_(*schema_store),
        ranking_strategy_(ScoringSpecProto::RankingStrategy::NONE),
        term_match_type_(TermMatchType::UNKNOWN) {}

  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
  TraverseTree(const Expr& parsed_expr, const std::string& property_filter,
               SectionRestrictQueryTermsMap* query_terms_out,
               QueryTermIteratorsMap* term_iterators_out);

  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
  TraverseTree(const Expr::Call& call, const std::string& property_filter,
               SectionRestrictQueryTermsMap* query_terms_out,
               QueryTermIteratorsMap* term_iterators_out);

  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>> HandleLeaf(
      const Constant& constant, const std::string& property_filter,
      SectionRestrictQueryTermsMap* query_terms_out,
      QueryTermIteratorsMap* term_iterators_out);

  // Not const because we could modify/sort the hit buffer in the lite index at
  // query time.
  Index& index_;
  const LanguageSegmenter& language_segmenter_;
  const Normalizer& normalizer_;
  const DocumentStore& document_store_;
  const SchemaStore& schema_store_;

  // Cached values
  DocHitInfoIteratorFilter::Options filter_options_;
  ScoringSpecProto::RankingStrategy::Code ranking_strategy_;
  TermMatchType::Code term_match_type_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_QUERY_ADVANCED_QUERY_PROCESSOR_H_
