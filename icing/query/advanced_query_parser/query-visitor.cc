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

#include "icing/query/advanced_query_parser/query-visitor.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/iterator/doc-hit-info-iterator-and.h"
#include "icing/index/iterator/doc-hit-info-iterator-none.h"
#include "icing/index/iterator/doc-hit-info-iterator-not.h"
#include "icing/index/iterator/doc-hit-info-iterator-or.h"
#include "icing/index/iterator/doc-hit-info-iterator-section-restrict.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/query/advanced_query_parser/lexer.h"
#include "icing/query/advanced_query_parser/param.h"
#include "icing/query/advanced_query_parser/parser.h"
#include "icing/query/query-features.h"
#include "icing/schema/property-util.h"
#include "icing/schema/section.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

struct CreateList {
  libtextclassifier3::StatusOr<PendingValue> operator()(
      std::vector<PendingValue>&& args) const {
    std::vector<std::string> values;
    values.reserve(args.size());
    for (PendingValue& arg : args) {
      std::string val = std::move(arg).string_val().ValueOrDie();
      values.push_back(std::move(val));
    }
    return PendingValue(std::move(values));
  }
};

libtextclassifier3::StatusOr<std::string> UnescapeStringValue(
    std::string_view value) {
  std::string result;
  bool in_escape = false;
  for (char c : value) {
    if (in_escape) {
      in_escape = false;
    } else if (c == '\\') {
      in_escape = true;
      continue;
    } else if (c == '"') {
      return absl_ports::InvalidArgumentError(
          "Encountered an unescaped quotation mark!");
    }
    result += c;
  }
  return result;
}

bool IsNumericComparator(std::string_view operator_text) {
  if (operator_text.length() < 1 || operator_text.length() > 2) {
    return false;
  }
  // TODO(tjbarron) decide how/if to support !=
  return operator_text == "<" || operator_text == ">" ||
         operator_text == "==" || operator_text == "<=" ||
         operator_text == ">=";
}

bool IsSupportedNaryOperator(std::string_view operator_text) {
  return IsNumericComparator(operator_text) || operator_text == "AND" ||
         operator_text == "OR" || operator_text == ":";
}

struct Int64Range {
  int64_t low;
  int64_t high;
};

libtextclassifier3::StatusOr<Int64Range> GetInt64Range(
    std::string_view operator_text, int64_t int_value) {
  Int64Range range = {std::numeric_limits<int64_t>::min(),
                      std::numeric_limits<int64_t>::max()};
  if (operator_text == "<") {
    if (int_value == std::numeric_limits<int64_t>::min()) {
      return absl_ports::InvalidArgumentError(
          "Cannot specify < INT64_MIN in query expression.");
    }
    range.high = int_value - 1;
  } else if (operator_text == "<=") {
    range.high = int_value;
  } else if (operator_text == "==") {
    range.high = int_value;
    range.low = int_value;
  } else if (operator_text == ">=") {
    range.low = int_value;
  } else if (operator_text == ">") {
    if (int_value == std::numeric_limits<int64_t>::max()) {
      return absl_ports::InvalidArgumentError(
          "Cannot specify > INT64_MAX in query expression.");
    }
    range.low = int_value + 1;
  }
  return range;
}

}  // namespace

void QueryVisitor::PendingPropertyRestricts::AddValidRestricts(
    std::set<std::string> new_restricts) {
  if (!has_active_property_restricts()) {
    pending_property_restricts_.push_back(std::move(new_restricts));
    return;
  }

  // There is an active property restrict already in effect. To determine the
  // updated active property restrict being applied at this level, we need to
  // calculate the intersection of new_restricts and
  // active_property_restricts.
  const std::set<std::string>& active_restricts = active_property_restricts();
  auto active_restricts_itr = active_restricts.begin();
  for (auto new_restricts_itr = new_restricts.begin();
       new_restricts_itr != new_restricts.end();) {
    while (active_restricts_itr != active_restricts.end() &&
           *active_restricts_itr < *new_restricts_itr) {
      // new_restricts_itr is behind active_restricts_itr.
      ++active_restricts_itr;
    }
    if (active_restricts_itr == active_restricts.end()) {
      // There's nothing left in active restricts. Everything at
      // new_restricts_itr and beyond should be removed
      new_restricts_itr =
          new_restricts.erase(new_restricts_itr, new_restricts.end());
    } else if (*active_restricts_itr > *new_restricts_itr) {
      // new_restricts_itr points to elements not present in
      // active_restricts_itr
      new_restricts_itr = new_restricts.erase(new_restricts_itr);
    } else {
      // the element that new_restricts_itr points to is present in
      // active_restricts_itr.
      ++new_restricts_itr;
    }
  }
  pending_property_restricts_.push_back(std::move(new_restricts));
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
QueryVisitor::CreateTermIterator(const std::string& term) {
  if (!processing_not_) {
    // 1. Add term to property_query_terms_map
    if (pending_property_restricts_.has_active_property_restricts()) {
      for (const std::string& property_restrict :
           pending_property_restricts_.active_property_restricts()) {
        property_query_terms_map_[property_restrict].insert(term);
      }
    } else {
      property_query_terms_map_[""].insert(term);
    }

    // 2. If needed add term iterator to query_term_iterators_ map.
    if (needs_term_frequency_info_) {
    // TODO(b/152934343) Save "term start index" into Node and PendingValue and
    // pass it into index.GetIterator
      ICING_ASSIGN_OR_RETURN(
          std::unique_ptr<DocHitInfoIterator> term_iterator,
          index_.GetIterator(term, /*term_start_index=*/0,
                             /*unnormalized_term_length=*/0, kSectionIdMaskAll,
                             match_type_, needs_term_frequency_info_));
      query_term_iterators_[term] = std::make_unique<DocHitInfoIteratorFilter>(
          std::move(term_iterator), &document_store_, &schema_store_,
          filter_options_);
    }
  }

  // 3. Add the term iterator.
  // TODO(b/208654892): Add support for the prefix operator (*).
  // TODO(b/152934343) Save "term start index" into Node and PendingValue and
  // pass it into index.GetIterator
  return index_.GetIterator(term, /*term_start_index=*/0,
                            /*unnormalized_term_length=*/0, kSectionIdMaskAll,
                            match_type_, needs_term_frequency_info_);
}

void QueryVisitor::RegisterFunctions() {
  // std::vector<std::string> createList(std::string...);
  Function create_list_function_ =
      Function::Create(DataType::kStringList, "createList",
                       {Param(DataType::kString, Cardinality::kRequired),
                        Param(DataType::kString, Cardinality::kVariable)},
                       CreateList())
          .ValueOrDie();
  registered_functions_.insert(
      {create_list_function_.name(), std::move(create_list_function_)});

  // DocHitInfoIterator search(std::string);
  // DocHitInfoIterator search(std::string, std::vector<std::string>);
  Function::EvalFunction search_eval =
      std::bind(&QueryVisitor::SearchFunction, this, std::placeholders::_1);
  Function search_function =
      Function::Create(DataType::kDocumentIterator, "search",
                       {Param(DataType::kString),
                        Param(DataType::kStringList, Cardinality::kOptional)},
                       std::move(search_eval))
          .ValueOrDie();
  registered_functions_.insert(
      {search_function.name(), std::move(search_function)});
}

libtextclassifier3::StatusOr<PendingValue> QueryVisitor::SearchFunction(
    std::vector<PendingValue>&& args) {
  // The second arg (if present) is a list of sections to restrict to.
  if (args.size() == 2) {
    std::set<std::string> new_restricts;
    std::vector<std::string> property_restricts =
        std::move(args.at(1)).string_vals().ValueOrDie();
    for (std::string& property_restrict : property_restricts) {
      new_restricts.insert(std::move(property_restrict));
    }
    pending_property_restricts_.AddValidRestricts(std::move(new_restricts));
    if (pending_property_restricts_.active_property_restricts().empty()) {
      pending_property_restricts_.PopRestricts();
      return PendingValue(std::make_unique<DocHitInfoIteratorNone>());
    }
  }

  // The first arg is guaranteed to be a STRING at this point. It should be safe
  // to call ValueOrDie.
  const std::string* query = args.at(0).string_val().ValueOrDie();
  Lexer lexer(*query, Lexer::Language::QUERY);
  ICING_ASSIGN_OR_RETURN(std::vector<Lexer::LexerToken> lexer_tokens,
                         lexer.ExtractTokens());

  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Node> tree_root,
                         parser.ConsumeQuery());

  std::unique_ptr<DocHitInfoIterator> iterator;
  QueryResults query_result;
  if (tree_root == nullptr) {
    iterator = std::make_unique<DocHitInfoIteratorAllDocumentId>(
        document_store_.last_added_document_id());
  } else {
    QueryVisitor query_visitor(&index_, &numeric_index_, &document_store_,
                               &schema_store_, &normalizer_, &tokenizer_,
                               filter_options_, match_type_,
                               needs_term_frequency_info_,
                               pending_property_restricts_, processing_not_);
    tree_root->Accept(&query_visitor);
    ICING_ASSIGN_OR_RETURN(query_result,
                           std::move(query_visitor).ConsumeResults());
    iterator = std::move(query_result.root_iterator);
  }

  // Update members based on results of processing the query.
  if (args.size() == 2 &&
      pending_property_restricts_.has_active_property_restricts()) {
    iterator = std::make_unique<DocHitInfoIteratorSectionRestrict>(
        std::move(iterator), &document_store_, &schema_store_,
        pending_property_restricts_.active_property_restricts());
    pending_property_restricts_.PopRestricts();
  }
  if (!processing_not_) {
    std::move(
        query_result.query_term_iterators.begin(),
        query_result.query_term_iterators.end(),
        std::inserter(query_term_iterators_, query_term_iterators_.end()));

    std::move(query_result.query_terms.begin(), query_result.query_terms.end(),
              std::inserter(property_query_terms_map_,
                            property_query_terms_map_.end()));
  }
  std::move(query_result.features_in_use.begin(),
            query_result.features_in_use.end(),
            std::inserter(features_, features_.end()));
  return PendingValue(std::move(iterator));
}

libtextclassifier3::StatusOr<int64_t> QueryVisitor::PopPendingIntValue() {
  if (pending_values_.empty()) {
    return absl_ports::InvalidArgumentError("Unable to retrieve int value.");
  }
  ICING_ASSIGN_OR_RETURN(int64_t int_value, pending_values_.top().long_val());
  pending_values_.pop();
  return int_value;
}

libtextclassifier3::StatusOr<std::string>
QueryVisitor::PopPendingStringValue() {
  if (pending_values_.empty()) {
    return absl_ports::InvalidArgumentError("Unable to retrieve string value.");
  }
  ICING_ASSIGN_OR_RETURN(std::string string_value,
                         std::move(pending_values_.top()).string_val());
  pending_values_.pop();
  return string_value;
}

libtextclassifier3::StatusOr<std::string> QueryVisitor::PopPendingTextValue() {
  if (pending_values_.empty()) {
    return absl_ports::InvalidArgumentError("Unable to retrieve text value.");
  }
  ICING_ASSIGN_OR_RETURN(std::string text_value,
                         std::move(pending_values_.top()).text_val());
  pending_values_.pop();
  return text_value;
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
QueryVisitor::PopPendingIterator() {
  if (pending_values_.empty() || pending_values_.top().is_placeholder()) {
    return absl_ports::InvalidArgumentError("Unable to retrieve iterator.");
  }
  if (pending_values_.top().data_type() == DataType::kDocumentIterator) {
    std::unique_ptr<DocHitInfoIterator> iterator =
        std::move(pending_values_.top()).iterator().ValueOrDie();
    pending_values_.pop();
    return iterator;
  } else if (pending_values_.top().data_type() == DataType::kString) {
    features_.insert(kVerbatimSearchFeature);
    ICING_ASSIGN_OR_RETURN(std::string value, PopPendingStringValue());
    return CreateTermIterator(std::move(value));
  } else {
    ICING_ASSIGN_OR_RETURN(std::string value, PopPendingTextValue());
    ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> token_itr,
                           tokenizer_.Tokenize(value));
    std::string normalized_term;
    std::vector<std::unique_ptr<DocHitInfoIterator>> iterators;
    while (token_itr->Advance()) {
      for (const Token& token : token_itr->GetTokens()) {
        normalized_term = normalizer_.NormalizeTerm(token.text);
        ICING_ASSIGN_OR_RETURN(std::unique_ptr<DocHitInfoIterator> iterator,
                               CreateTermIterator(std::move(normalized_term)));
        iterators.push_back(std::move(iterator));
      }
    }

    // Finally, create an And Iterator. If there's only a single term here, then
    // it will just return that term iterator. Otherwise, segmented text is
    // treated as a group of terms AND'd together.
    return CreateAndIterator(std::move(iterators));
  }
}

libtextclassifier3::StatusOr<std::vector<std::unique_ptr<DocHitInfoIterator>>>
QueryVisitor::PopAllPendingIterators() {
  std::vector<std::unique_ptr<DocHitInfoIterator>> iterators;
  while (!pending_values_.empty() && !pending_values_.top().is_placeholder()) {
    ICING_ASSIGN_OR_RETURN(std::unique_ptr<DocHitInfoIterator> itr,
                           PopPendingIterator());
    iterators.push_back(std::move(itr));
  }
  if (pending_values_.empty()) {
    return absl_ports::InvalidArgumentError(
        "Unable to retrieve expected iterators.");
  }
  // Iterators will be in reverse order because we retrieved them from the
  // stack. Reverse them to get back to the original ordering.
  std::reverse(iterators.begin(), iterators.end());
  return iterators;
}

libtextclassifier3::StatusOr<PendingValue>
QueryVisitor::ProcessNumericComparator(const NaryOperatorNode* node) {
  // 1. The children should have been processed and added their outputs to
  // pending_values_. Time to process them.
  // The first two pending values should be the int value and the property.
  ICING_ASSIGN_OR_RETURN(int64_t int_value, PopPendingIntValue());
  ICING_ASSIGN_OR_RETURN(std::string property, PopPendingTextValue());

  // 2. Create the iterator.
  ICING_ASSIGN_OR_RETURN(Int64Range range,
                         GetInt64Range(node->operator_text(), int_value));
  auto iterator_or =
      numeric_index_.GetIterator(property, range.low, range.high);
  if (!iterator_or.ok()) {
    return std::move(iterator_or).status();
  }

  features_.insert(kNumericSearchFeature);
  std::unique_ptr<DocHitInfoIterator> iterator =
      std::move(iterator_or).ValueOrDie();
  return PendingValue(std::move(iterator));
}

libtextclassifier3::StatusOr<PendingValue> QueryVisitor::ProcessAndOperator(
    const NaryOperatorNode* node) {
  ICING_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<DocHitInfoIterator>> iterators,
      PopAllPendingIterators());
  return PendingValue(CreateAndIterator(std::move(iterators)));
}

libtextclassifier3::StatusOr<PendingValue> QueryVisitor::ProcessOrOperator(
    const NaryOperatorNode* node) {
  ICING_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<DocHitInfoIterator>> iterators,
      PopAllPendingIterators());
  return PendingValue(CreateOrIterator(std::move(iterators)));
}

libtextclassifier3::Status QueryVisitor::ProcessHasOperator(
    const NaryOperatorNode* node) {
  if (node->children().size() != 2) {
    return absl_ports::InvalidArgumentError("Expected 2 children.");
  }

  // 1. Put in a placeholder PendingValue
  pending_values_.push(PendingValue());

  // 2. Visit the first child - the property.
  node->children().at(0)->Accept(this);
  if (has_pending_error()) {
    return pending_error_;
  }
  ICING_ASSIGN_OR_RETURN(std::string property, PopPendingTextValue());
  pending_property_restricts_.AddValidRestricts({property});

  // Just added a restrict - if there are no active property restricts then that
  // be because this restrict is unsatisfiable.
  if (pending_property_restricts_.active_property_restricts().empty()) {
    // The property restrict can't be satisfiable. Pop the placeholder that was
    // just added and push a FALSE iterator.
    pending_property_restricts_.PopRestricts();
    pending_values_.pop();
    pending_values_.push(
        PendingValue(std::make_unique<DocHitInfoIteratorNone>()));
    return libtextclassifier3::Status::OK;
  }

  // 3. Visit the second child - the argument.
  node->children().at(1)->Accept(this);
  if (has_pending_error()) {
    return pending_error_;
  }
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<DocHitInfoIterator> delegate,
                         PopPendingIterator());

  // 4. Check for the placeholder.
  if (!pending_values_.top().is_placeholder()) {
    return absl_ports::InvalidArgumentError(
        "Error processing arguments for node.");
  }
  pending_values_.pop();
  pending_property_restricts_.PopRestricts();

  std::set<std::string> property_restricts = {std::move(property)};
  pending_values_.push(
      PendingValue(std::make_unique<DocHitInfoIteratorSectionRestrict>(
          std::move(delegate), &document_store_, &schema_store_,
          std::move(property_restricts))));
  return libtextclassifier3::Status::OK;
}

void QueryVisitor::VisitFunctionName(const FunctionNameNode* node) {
  pending_error_ = absl_ports::UnimplementedError(
      "Function Name node visiting not implemented yet.");
}

void QueryVisitor::VisitString(const StringNode* node) {
  // A STRING node can only be a term. Create the iterator now.
  auto unescaped_string_or = UnescapeStringValue(node->value());
  if (!unescaped_string_or.ok()) {
    pending_error_ = std::move(unescaped_string_or).status();
    return;
  }
  std::string unescaped_string = std::move(unescaped_string_or).ValueOrDie();
  pending_values_.push(
      PendingValue::CreateStringPendingValue(std::move(unescaped_string)));
}

void QueryVisitor::VisitText(const TextNode* node) {
  // TEXT nodes could either be a term (and will become DocHitInfoIteratorTerm)
  // or a property name. As such, we just push the TEXT value into pending
  // values and determine which it is at a later point.
  pending_values_.push(
      PendingValue::CreateTextPendingValue(std::move(node->value())));
}

void QueryVisitor::VisitMember(const MemberNode* node) {
  if (node->children().empty()) {
    pending_error_ =
        absl_ports::InvalidArgumentError("Encountered malformed member node.");
    return;
  }

  // 1. Put in a placeholder PendingValue
  pending_values_.push(PendingValue());

  // 2. Visit the children.
  for (const std::unique_ptr<TextNode>& child : node->children()) {
    child->Accept(this);
    if (has_pending_error()) {
      return;
    }
  }

  // 3. The children should have been processed and added their outputs to
  // pending_values_. Time to process them.
  libtextclassifier3::StatusOr<std::string> member_or;
  std::vector<std::string> members;
  while (!pending_values_.empty() && !pending_values_.top().is_placeholder()) {
    member_or = PopPendingTextValue();
    if (!member_or.ok()) {
      pending_error_ = std::move(member_or).status();
      return;
    }
    members.push_back(std::move(member_or).ValueOrDie());
  }
  std::string member =
      absl_ports::StrJoin(members.rbegin(), members.rend(),
      property_util::kPropertyPathSeparator);

  // 4. If pending_values_ is empty somehow, then our placeholder disappeared
  // somehow.
  if (pending_values_.empty()) {
    pending_error_ = absl_ports::InvalidArgumentError(
        "Error processing arguments for member node.");
    return;
  }
  pending_values_.pop();

  pending_values_.push(PendingValue::CreateTextPendingValue(std::move(member)));
}

void QueryVisitor::VisitFunction(const FunctionNode* node) {
  // 1. Get the associated function.
  auto itr = registered_functions_.find(node->function_name()->value());
  if (itr == registered_functions_.end()) {
    pending_error_ = absl_ports::InvalidArgumentError(absl_ports::StrCat(
        "Function ", node->function_name()->value(), " is not supported."));
    return;
  }

  // 2. Put in a placeholder PendingValue
  pending_values_.push(PendingValue());

  // 3. Visit the children.
  for (const std::unique_ptr<Node>& arg : node->args()) {
    arg->Accept(this);
    if (has_pending_error()) {
      return;
    }
  }

  // 4. Collect the arguments and evaluate the function.
  std::vector<PendingValue> args;
  while (!pending_values_.empty() && !pending_values_.top().is_placeholder()) {
    args.push_back(std::move(pending_values_.top()));
    pending_values_.pop();
  }
  std::reverse(args.begin(), args.end());
  const Function& function = itr->second;
  auto eval_result = function.Eval(std::move(args));
  if (!eval_result.ok()) {
    pending_error_ = std::move(eval_result).status();
    return;
  }

  // 5. Pop placeholder in pending_values and add the result of our function.
  pending_values_.pop();
  pending_values_.push(std::move(eval_result).ValueOrDie());
}

// TODO(b/265312785) Clarify handling of the interaction between HAS and NOT.
// Currently, `prop1:(NOT foo bar)` will not match any documents. Likewise,
// `search("NOT foo bar", createList("prop1"))` will not match any documents.
//
// We should either confirm that this is the desired behavior or consider
// rewriting these queries so that they're interpreted as
// `NOT prop1:foo AND prop1:bar` and
// `NOT search("foo", createList("prop1"))
//  AND search("bar", createList("prop1"))`
void QueryVisitor::VisitUnaryOperator(const UnaryOperatorNode* node) {
  if (node->operator_text() != "NOT") {
    pending_error_ = absl_ports::UnimplementedError(
        absl_ports::StrCat("Visiting for unary operator ",
                           node->operator_text(), " not implemented yet."));
    return;
  }

  // TODO(b/265312785) Consider implementing query optimization when we run into
  // nested NOTs. This would allow us to simplify a query like "NOT (-foo)" to
  // just "foo". This would also require more complicate rewrites as we would
  // need to do things like rewrite "NOT (-a OR b)" as "a AND -b" and
  // "NOT (price < 5)" as "price >= 5".
  // 1. Put in a placeholder PendingValue
  pending_values_.push(PendingValue());
  // Toggle whatever the current value of 'processing_not_' is before visiting
  // the children.
  processing_not_ = !processing_not_;

  // 2. Visit child
  node->child()->Accept(this);
  if (has_pending_error()) {
    return;
  }

  if (pending_values_.size() < 2) {
    pending_error_ = absl_ports::InvalidArgumentError(
        "Visit unary operator child didn't correctly add pending values.");
    return;
  }

  // 3. Retrieve the delegate iterator
  auto iterator_or = PopPendingIterator();
  if (!iterator_or.ok()) {
    pending_error_ = std::move(iterator_or).status();
    return;
  }
  std::unique_ptr<DocHitInfoIterator> delegate =
      std::move(iterator_or).ValueOrDie();

  // 4. Check for the placeholder.
  if (!pending_values_.top().is_placeholder()) {
    pending_error_ = absl_ports::InvalidArgumentError(
        "Error processing arguments for node.");
    return;
  }
  pending_values_.pop();

  pending_values_.push(PendingValue(std::make_unique<DocHitInfoIteratorNot>(
      std::move(delegate), document_store_.last_added_document_id())));

  // Untoggle whatever the current value of 'processing_not_' is now that we've
  // finished processing this NOT.
  processing_not_ = !processing_not_;
}

void QueryVisitor::VisitNaryOperator(const NaryOperatorNode* node) {
  if (!IsSupportedNaryOperator(node->operator_text())) {
    pending_error_ = absl_ports::UnimplementedError(
        "No support for any non-numeric operators.");
    return;
  }

  if (node->operator_text() == ":") {
    libtextclassifier3::Status status = ProcessHasOperator(node);
    if (!status.ok()) {
      pending_error_ = std::move(status);
    }
    return;
  }

  // 1. Put in a placeholder PendingValue
  pending_values_.push(PendingValue());

  // 2. Visit the children.
  for (int i = 0; i < node->children().size(); ++i) {
    node->children().at(i)->Accept(this);
    if (has_pending_error()) {
      return;
    }
  }

  // 3. Retrieve the pending value for this node.
  libtextclassifier3::StatusOr<PendingValue> pending_value_or;
  if (IsNumericComparator(node->operator_text())) {
    pending_value_or = ProcessNumericComparator(node);
  } else if (node->operator_text() == "AND") {
    pending_value_or = ProcessAndOperator(node);
  } else if (node->operator_text() == "OR") {
    pending_value_or = ProcessOrOperator(node);
  }
  if (!pending_value_or.ok()) {
    pending_error_ = std::move(pending_value_or).status();
    return;
  }
  PendingValue pending_value = std::move(pending_value_or).ValueOrDie();

  // 4. Check for the placeholder.
  if (!pending_values_.top().is_placeholder()) {
    pending_error_ = absl_ports::InvalidArgumentError(
        "Error processing arguments for node.");
    return;
  }
  pending_values_.pop();

  pending_values_.push(std::move(pending_value));
}

libtextclassifier3::StatusOr<QueryResults> QueryVisitor::ConsumeResults() && {
  if (has_pending_error()) {
    return std::move(pending_error_);
  }
  if (pending_values_.size() != 1) {
    return absl_ports::InvalidArgumentError(
        "Visitor does not contain a single root iterator.");
  }
  auto iterator_or = PopPendingIterator();
  if (!iterator_or.ok()) {
    return std::move(iterator_or).status();
  }

  QueryResults results;
  results.root_iterator = std::move(iterator_or).ValueOrDie();
  results.query_term_iterators = std::move(query_term_iterators_);
  results.query_terms = std::move(property_query_terms_map_);
  results.features_in_use = std::move(features_);
  return results;
}

}  // namespace lib
}  // namespace icing
