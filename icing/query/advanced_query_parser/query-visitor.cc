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

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/iterator/doc-hit-info-iterator-and.h"
#include "icing/index/iterator/doc-hit-info-iterator-not.h"
#include "icing/index/iterator/doc-hit-info-iterator-or.h"
#include "icing/index/iterator/doc-hit-info-iterator-section-restrict.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/query/query-features.h"
#include "icing/schema/section-manager.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

libtextclassifier3::StatusOr<std::string> EscapeStringValue(
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

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
QueryVisitor::CreateTermIterator(const std::string& term) {
  if (!processing_not_) {
    // 1. Add term to property_query_terms_map
    auto property_restrict_or = GetPropertyRestrict();
    if (property_restrict_or.ok()) {
      std::string property_restrict =
          std::move(property_restrict_or).ValueOrDie();
      property_query_terms_map_[std::move(property_restrict)].insert(term);
    } else {
      ICING_LOG(DBG) << "Unsatisfiable property restrict, "
                     << property_restrict_or.status().error_message();
    }

    // 2. If needed add term iterator to query_term_iterators_ map.
    if (needs_term_frequency_info_) {
      ICING_ASSIGN_OR_RETURN(
          std::unique_ptr<DocHitInfoIterator> term_iterator,
          index_.GetIterator(term, kSectionIdMaskAll, match_type_,
                             needs_term_frequency_info_));
      query_term_iterators_[term] = std::make_unique<DocHitInfoIteratorFilter>(
          std::move(term_iterator), &document_store_, &schema_store_,
          filter_options_);
    }
  }

  // 3. Add the term iterator.
  // TODO(b/208654892): Add support for the prefix operator (*).
  return index_.GetIterator(term, kSectionIdMaskAll, match_type_,
                            needs_term_frequency_info_);
}

libtextclassifier3::StatusOr<int64_t> QueryVisitor::PopPendingIntValue() {
  if (pending_values_.empty() ||
      pending_values_.top().data_type() != PendingValue::DataType::kText) {
    return absl_ports::InvalidArgumentError("Unable to retrieve int value.");
  }
  const std::string& value = pending_values_.top().term();
  char* value_end;
  int64_t int_value = std::strtoll(value.c_str(), &value_end, /*base=*/10);
  if (value_end != value.c_str() + value.length()) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat("Unable to parse \"", value, "\" as number."));
  }
  pending_values_.pop();
  return int_value;
}

libtextclassifier3::StatusOr<std::string>
QueryVisitor::PopPendingStringValue() {
  if (pending_values_.empty() ||
      pending_values_.top().data_type() != PendingValue::DataType::kString) {
    return absl_ports::InvalidArgumentError("Unable to retrieve text value.");
  }
  std::string string_value = std::move(pending_values_.top().term());
  pending_values_.pop();
  return string_value;
}

libtextclassifier3::StatusOr<std::string> QueryVisitor::PopPendingTextValue() {
  if (pending_values_.empty() ||
      pending_values_.top().data_type() != PendingValue::DataType::kText) {
    return absl_ports::InvalidArgumentError("Unable to retrieve text value.");
  }
  std::string text_value = std::move(pending_values_.top().term());
  pending_values_.pop();
  return text_value;
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
QueryVisitor::PopPendingIterator() {
  if (pending_values_.empty() || pending_values_.top().is_placeholder()) {
    return absl_ports::InvalidArgumentError("Unable to retrieve iterator.");
  }
  if (pending_values_.top().data_type() ==
      PendingValue::DataType::kDocIterator) {
    std::unique_ptr<DocHitInfoIterator> iterator =
        std::move(pending_values_.top().iterator());
    pending_values_.pop();
    return iterator;
  } else if (pending_values_.top().data_type() ==
             PendingValue::DataType::kString) {
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

libtextclassifier3::StatusOr<QueryVisitor::PendingValue>
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

libtextclassifier3::StatusOr<QueryVisitor::PendingValue>
QueryVisitor::ProcessAndOperator(const NaryOperatorNode* node) {
  ICING_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<DocHitInfoIterator>> iterators,
      PopAllPendingIterators());
  return PendingValue(CreateAndIterator(std::move(iterators)));
}

libtextclassifier3::StatusOr<QueryVisitor::PendingValue>
QueryVisitor::ProcessOrOperator(const NaryOperatorNode* node) {
  ICING_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<DocHitInfoIterator>> iterators,
      PopAllPendingIterators());
  return PendingValue(CreateOrIterator(std::move(iterators)));
}

libtextclassifier3::StatusOr<QueryVisitor::PendingValue>
QueryVisitor::ProcessHasOperator(const NaryOperatorNode* node) {
  // The children should have been processed and added their outputs to
  // pending_values_. Time to process them.
  // The first two pending values should be the delegate and the property.
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<DocHitInfoIterator> delegate,
                         PopPendingIterator());
  ICING_ASSIGN_OR_RETURN(std::string property, PopPendingTextValue());
  return PendingValue(std::make_unique<DocHitInfoIteratorSectionRestrict>(
      std::move(delegate), &document_store_, &schema_store_,
      std::move(property)));
}

libtextclassifier3::StatusOr<std::string> QueryVisitor::GetPropertyRestrict()
    const {
  if (pending_property_restricts_.empty()) {
    return "";
  }
  const std::string& restrict = pending_property_restricts_.at(0);
  bool valid_restrict = std::all_of(
      pending_property_restricts_.begin(), pending_property_restricts_.end(),
      [&restrict](const std::string& s) { return s == restrict; });
  if (!valid_restrict) {
    return absl_ports::InvalidArgumentError(
        "Invalid property restrict provided!");
  }
  return pending_property_restricts_.at(0);
}

void QueryVisitor::VisitFunctionName(const FunctionNameNode* node) {
  pending_error_ = absl_ports::UnimplementedError(
      "Function Name node visiting not implemented yet.");
}

void QueryVisitor::VisitString(const StringNode* node) {
  // A STRING node can only be a term. Create the iterator now.
  auto escaped_string_or = EscapeStringValue(node->value());
  if (!escaped_string_or.ok()) {
    pending_error_ = std::move(escaped_string_or).status();
    return;
  }
  pending_values_.push(PendingValue::CreateStringPendingValue(
      std::move(escaped_string_or).ValueOrDie()));
}

void QueryVisitor::VisitText(const TextNode* node) {
  // TEXT nodes could either be a term (and will become DocHitInfoIteratorTerm)
  // or a property name. As such, we just push the TEXT value into pending
  // values and determine which it is at a later point.
  pending_values_.push(
      PendingValue::CreateTextPendingValue(std::move(node->value())));
}

void QueryVisitor::VisitMember(const MemberNode* node) {
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
  std::string member = std::move(pending_values_.top().term());
  pending_values_.pop();
  while (!pending_values_.empty() && !pending_values_.top().is_placeholder()) {
    member = absl_ports::StrCat(pending_values_.top().term(),
                                kPropertySeparator, member);
    pending_values_.pop();
  }

  // 4. If pending_values_ is empty somehow, then our placeholder disappeared
  // somehow.
  if (pending_values_.empty()) {
    pending_error_ = absl_ports::InvalidArgumentError(
        "\"<\" operator must have two arguments.");
    return;
  }
  pending_values_.pop();

  pending_values_.push(PendingValue::CreateTextPendingValue(std::move(member)));
}

void QueryVisitor::VisitFunction(const FunctionNode* node) {
  pending_error_ = absl_ports::UnimplementedError(
      "Function node visiting not implemented yet.");
}

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

  // 1. Put in a placeholder PendingValue
  pending_values_.push(PendingValue());

  // 2. Visit the children.
  bool processing_has = node->operator_text() == ":";
  for (int i = 0; i < node->children().size(); ++i) {
    node->children().at(i)->Accept(this);
    if (has_pending_error()) {
      return;
    }
    if (processing_has && !processing_not_ && i == 0) {
      if (pending_values_.top().data_type() != PendingValue::DataType::kText) {
        pending_error_ = absl_ports::InvalidArgumentError(
            "Expected property before ':' operator.");
        return;
      }
      pending_property_restricts_.push_back(pending_values_.top().term());
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
  } else if (processing_has) {
    pending_value_or = ProcessHasOperator(node);
    if (!processing_not_) {
      pending_property_restricts_.pop_back();
    }
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
