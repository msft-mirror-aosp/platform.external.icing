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

#include "icing/query/advanced-query-processor.h"

#include "icing/index/iterator/doc-hit-info-iterator-all-document-id.h"
#include "icing/index/iterator/doc-hit-info-iterator-and.h"
#include "icing/index/iterator/doc-hit-info-iterator-filter.h"
#include "icing/index/iterator/doc-hit-info-iterator-not.h"
#include "icing/index/iterator/doc-hit-info-iterator-or.h"
#include "icing/index/iterator/doc-hit-info-iterator-section-restrict.h"
#include "icing/query/list_filter/list-filter-parser-factory.h"
#include "icing/query/list_filter/list-filter-parser.h"
#include "icing/query/query-terms.h"
#include "icing/query/query-utils.h"
#include "icing/schema/section-manager.h"

namespace icing {
namespace lib {

namespace {

void AddTermRestrict(SectionRestrictQueryTermsMap& query_terms,
                     const std::string& section, std::string_view term) {
  auto itr = query_terms.find(section);
  if (itr == query_terms.end()) {
    query_terms.insert({section, {std::string(term)}});
  } else {
    itr->second.insert(std::string(term));
  }
}

std::string RetrievePropertyPath(const Expr& expr) {
  // Two things about the way paths are stored in the parser make it difficult
  // reconstruct the path.
  // 1. The path is in reverse order, so "recipient.contact.email" would be
  //    provided in order of ["email", "contact", "recipient"]
  // 2. The base object ("recipient" in the example) is an Identity, while the
  //    subsequent fields are Selects. So "recipient.contact.email" would be
  //    provided as [ Select{"email"}, Select{"contact"}, Ident{"recipient"} ]
  const Expr* cur_expr = &expr;
  std::vector<std::string_view> path_parts;
  while (cur_expr->expr_kind_case() != Expr::kIdentExpr) {
    // This must be part of a nested path. Keep processing the Selects until we
    // reach the base (the Ident).
    path_parts.push_back(cur_expr->select_expr().field());
    cur_expr = &cur_expr->select_expr().operand();
  }
  path_parts.push_back(cur_expr->ident_expr().name());
  return absl_ports::StrJoin(path_parts.rbegin(), path_parts.rend(),
                             kPropertySeparator,
                             absl_ports::DefaultFormatter());
}

}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<AdvancedQueryProcessor>>
AdvancedQueryProcessor::Create(Index* index,
                               const LanguageSegmenter* language_segmenter,
                               const Normalizer* normalizer,
                               const DocumentStore* document_store,
                               const SchemaStore* schema_store) {
  ICING_RETURN_ERROR_IF_NULL(index);
  ICING_RETURN_ERROR_IF_NULL(language_segmenter);
  ICING_RETURN_ERROR_IF_NULL(normalizer);
  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);

  return std::unique_ptr<AdvancedQueryProcessor>(new AdvancedQueryProcessor(
      index, language_segmenter, normalizer, document_store, schema_store));
}

libtextclassifier3::StatusOr<QueryResults> AdvancedQueryProcessor::ParseSearch(
    const SearchSpecProto& search_spec,
    ScoringSpecProto::RankingStrategy::Code ranking_strategy) {
  filter_options_ = GetFilterOptions(search_spec);
  term_match_type_ = search_spec.term_match_type();
  ranking_strategy_ = ranking_strategy;

  QueryResults results;
  std::unique_ptr<ListFilterParser> parser = list_filter_parser_factory::Create(
      list_filter_parser_factory::ParserType::kCloud);
  Expr parsed_expr = parser->Parse(search_spec.query());
  QueryTermIteratorsMap* query_term_iterators =
      (ranking_strategy_ == ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE)
          ? &results.query_term_iterators
          : nullptr;
  ICING_ASSIGN_OR_RETURN(results.root_iterator,
                         TraverseTree(parsed_expr, "", &results.query_terms,
                                      query_term_iterators));
  return results;
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
AdvancedQueryProcessor::HandleLeaf(const Constant& constant,
                                   const std::string& property_filter,
                                   SectionRestrictQueryTermsMap* query_terms,
                                   QueryTermIteratorsMap* term_iterators) {
  std::string normalized_text =
      normalizer_.NormalizeTerm(constant.string_value());
  if (term_iterators != nullptr) {
    // Only need to populate term iterators if we're doing relevance scoring.
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<DocHitInfoIterator> term_iterator,
        index_.GetIterator(
            normalized_text, kSectionIdMaskAll, term_match_type_,
            /*need_hit_term_frequency=*/ranking_strategy_ ==
                ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));
    term_iterator = std::make_unique<DocHitInfoIteratorFilter>(
        std::move(term_iterator), &document_store_, &schema_store_,
        filter_options_);
    (*term_iterators)[normalized_text] = std::move(term_iterator);
  }

  if (query_terms != nullptr) {
    AddTermRestrict(*query_terms, property_filter, normalized_text);
  }

  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<DocHitInfoIterator> term_iterator,
      index_.GetIterator(
          normalized_text, kSectionIdMaskAll, term_match_type_,
          /*need_hit_term_frequency=*/ranking_strategy_ ==
              ScoringSpecProto::RankingStrategy::RELEVANCE_SCORE));
  if (!property_filter.empty()) {
    term_iterator = std::make_unique<DocHitInfoIteratorSectionRestrict>(
        std::move(term_iterator), &document_store_, &schema_store_,
        property_filter);
  }
  return term_iterator;
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
AdvancedQueryProcessor::TraverseTree(const Expr::Call& call,
                                     const std::string& property_filter,
                                     SectionRestrictQueryTermsMap* query_terms,
                                     QueryTermIteratorsMap* term_iterators) {
  if (call.function() == "_&&_") {
    std::vector<std::unique_ptr<DocHitInfoIterator>> sub_itrs;
    sub_itrs.reserve(call.args_size());
    for (const Expr& arg_expr : call.args()) {
      ICING_ASSIGN_OR_RETURN(
          std::unique_ptr<DocHitInfoIterator> sub_itr,
          TraverseTree(arg_expr, property_filter, query_terms, term_iterators));
      sub_itrs.push_back(std::move(sub_itr));
    }
    return CreateAndIterator(std::move(sub_itrs));
  } else if (call.function() == "_||_") {
    std::vector<std::unique_ptr<DocHitInfoIterator>> sub_itrs;
    sub_itrs.reserve(call.args_size());
    for (const Expr& arg_expr : call.args()) {
      ICING_ASSIGN_OR_RETURN(
          std::unique_ptr<DocHitInfoIterator> sub_itr,
          TraverseTree(arg_expr, property_filter, query_terms, term_iterators));
      sub_itrs.push_back(std::move(sub_itr));
    }
    return CreateOrIterator(std::move(sub_itrs));
  } else if (call.function() == "NOT") {
    ICING_ASSIGN_OR_RETURN(std::unique_ptr<DocHitInfoIterator> sub_itr,
                           TraverseTree(call.args(0), property_filter,
                                        /*query_terms=*/nullptr,
                                        /*term_iterators=*/nullptr));
    return std::make_unique<DocHitInfoIteratorNot>(
        std::move(sub_itr), document_store_.last_added_document_id());
  } else if (call.function() == ":") {
    std::string property_path = RetrievePropertyPath(call.args(0));
    return TraverseTree(call.args(1), property_path, query_terms,
                        term_iterators);
  } else {
    return absl_ports::UnimplementedError(IcingStringUtil::StringPrintf(
        "Function %s NOT IMPLEMENTED", call.function().c_str()));
  }
}

// Traverse the subtree starting at parsed_expr.
libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
AdvancedQueryProcessor::TraverseTree(const Expr& parsed_expr,
                                     const std::string& property_filter,
                                     SectionRestrictQueryTermsMap* query_terms,
                                     QueryTermIteratorsMap* term_iterators) {
  switch (parsed_expr.expr_kind_case()) {
    case Expr::kConstExpr:
      return HandleLeaf(parsed_expr.const_expr(), property_filter, query_terms,
                        term_iterators);
    case Expr::kCallExpr:
      return TraverseTree(parsed_expr.call_expr(), property_filter, query_terms,
                          term_iterators);
    case Expr::EXPR_KIND_NOT_SET:
      return std::make_unique<DocHitInfoIteratorAllDocumentId>(
          document_store_.last_added_document_id());
    case Expr::kSelectExpr:
    case Expr::kIdentExpr:
      return absl_ports::UnimplementedError(IcingStringUtil::StringPrintf(
          "Expression type %d NOT IMPLEMENTED",
          static_cast<int>(parsed_expr.expr_kind_case())));
  }
}

}  // namespace lib
}  // namespace icing
