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

#include "icing/query/query-processor.h"

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator-all-document-id.h"
#include "icing/index/iterator/doc-hit-info-iterator-filter.h"
#include "icing/index/iterator/doc-hit-info-iterator-section-restrict.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"
#include "icing/query/advanced_query_parser/lexer.h"
#include "icing/query/advanced_query_parser/parser.h"
#include "icing/query/advanced_query_parser/query-visitor.h"
#include "icing/query/query-features.h"
#include "icing/query/query-results.h"
#include "icing/query/query-utils.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-store.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/tokenization/tokenizer-factory.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/transform/normalizer.h"
#include "icing/util/clock.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<QueryProcessor>>
QueryProcessor::Create(Index* index, const NumericIndex<int64_t>* numeric_index,
                       const EmbeddingIndex* embedding_index,
                       const LanguageSegmenter* language_segmenter,
                       const Normalizer* normalizer,
                       const DocumentStore* document_store,
                       const SchemaStore* schema_store, const Clock* clock) {
  ICING_RETURN_ERROR_IF_NULL(index);
  ICING_RETURN_ERROR_IF_NULL(numeric_index);
  ICING_RETURN_ERROR_IF_NULL(embedding_index);
  ICING_RETURN_ERROR_IF_NULL(language_segmenter);
  ICING_RETURN_ERROR_IF_NULL(normalizer);
  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(clock);

  return std::unique_ptr<QueryProcessor>(new QueryProcessor(
      index, numeric_index, embedding_index, language_segmenter, normalizer,
      document_store, schema_store, clock));
}

QueryProcessor::QueryProcessor(Index* index,
                               const NumericIndex<int64_t>* numeric_index,
                               const EmbeddingIndex* embedding_index,
                               const LanguageSegmenter* language_segmenter,
                               const Normalizer* normalizer,
                               const DocumentStore* document_store,
                               const SchemaStore* schema_store,
                               const Clock* clock)
    : index_(*index),
      numeric_index_(*numeric_index),
      embedding_index_(*embedding_index),
      language_segmenter_(*language_segmenter),
      normalizer_(*normalizer),
      document_store_(*document_store),
      schema_store_(*schema_store),
      clock_(*clock) {}

libtextclassifier3::StatusOr<QueryResults> QueryProcessor::ParseSearch(
    const SearchSpecProto& search_spec,
    ScoringSpecProto::RankingStrategy::Code ranking_strategy,
    int64_t current_time_ms, QueryStatsProto::SearchStats* search_stats) {
  ICING_ASSIGN_OR_RETURN(QueryResults results,
                         ParseAdvancedQuery(search_spec, ranking_strategy,
                                            current_time_ms, search_stats));

  // Check that all new features used in the search have been enabled in the
  // SearchSpec.
  const std::unordered_set<Feature> enabled_features(
      search_spec.enabled_features().begin(),
      search_spec.enabled_features().end());
  for (const Feature feature : results.features_in_use) {
    if (enabled_features.find(feature) == enabled_features.end()) {
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "Attempted use of unenabled feature ", feature,
          ". Please make sure that you have explicitly set all advanced query "
          "features used in this query as enabled in the SearchSpec."));
    }
  }

  DocHitInfoIteratorFilter::Options options = GetFilterOptions(search_spec);
  results.root_iterator = std::make_unique<DocHitInfoIteratorFilter>(
      std::move(results.root_iterator), &document_store_, &schema_store_,
      options, current_time_ms);
  if (!search_spec.type_property_filters().empty()) {
    results.root_iterator =
        DocHitInfoIteratorSectionRestrict::ApplyRestrictions(
            std::move(results.root_iterator), &document_store_, &schema_store_,
            search_spec, current_time_ms);
  }
  return results;
}

libtextclassifier3::StatusOr<QueryResults> QueryProcessor::ParseAdvancedQuery(
    const SearchSpecProto& search_spec,
    ScoringSpecProto::RankingStrategy::Code ranking_strategy,
    int64_t current_time_ms, QueryStatsProto::SearchStats* search_stats) const {
  std::unique_ptr<Timer> lexer_timer = clock_.GetNewTimer();
  Lexer lexer(search_spec.query(), Lexer::Language::QUERY);
  ICING_ASSIGN_OR_RETURN(std::vector<Lexer::LexerToken> lexer_tokens,
                         lexer.ExtractTokens());
  if (search_stats != nullptr) {
    search_stats->set_query_processor_lexer_extract_token_latency_ms(
        lexer_timer->GetElapsedMilliseconds());
  }

  std::unique_ptr<Timer> parser_timer = clock_.GetNewTimer();
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Node> tree_root,
                         parser.ConsumeQuery());
  if (search_stats != nullptr) {
    search_stats->set_query_processor_parser_consume_query_latency_ms(
        parser_timer->GetElapsedMilliseconds());
  }

  if (tree_root == nullptr) {
    QueryResults results;
    results.root_iterator = std::make_unique<DocHitInfoIteratorAllDocumentId>(
        document_store_.last_added_document_id());
    return results;
  }
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<Tokenizer> plain_tokenizer,
      tokenizer_factory::CreateIndexingTokenizer(
          StringIndexingConfig::TokenizerType::PLAIN, &language_segmenter_));
  DocHitInfoIteratorFilter::Options options = GetFilterOptions(search_spec);
  bool needs_term_frequency_info =
      ranking_strategy == ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE;

  std::unique_ptr<Timer> query_visitor_timer = clock_.GetNewTimer();
  QueryVisitor query_visitor(
      &index_, &numeric_index_, &embedding_index_, &document_store_,
      &schema_store_, &normalizer_, plain_tokenizer.get(), search_spec.query(),
      &search_spec.embedding_query_vectors(), std::move(options),
      search_spec.term_match_type(), search_spec.embedding_query_metric_type(),
      needs_term_frequency_info, current_time_ms);
  tree_root->Accept(&query_visitor);
  ICING_ASSIGN_OR_RETURN(QueryResults results,
                         std::move(query_visitor).ConsumeResults());
  if (search_stats != nullptr) {
    search_stats->set_query_processor_query_visitor_latency_ms(
        query_visitor_timer->GetElapsedMilliseconds());
  }

  return results;
}

}  // namespace lib
}  // namespace icing
