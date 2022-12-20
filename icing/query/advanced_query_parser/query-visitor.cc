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

libtextclassifier3::StatusOr<int64_t> QueryVisitor::RetrieveIntValue() {
  if (pending_values_.empty() || !pending_values_.top().holds_text()) {
    return absl_ports::InvalidArgumentError("Unable to retrieve int value.");
  }
  std::string& value = pending_values_.top().text;
  char* value_end;
  int64_t int_value = std::strtoll(value.c_str(), &value_end, /*base=*/10);
  if (value_end != value.c_str() + value.length()) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat("Unable to parse \"", value, "\" as number."));
  }
  pending_values_.pop();
  return int_value;
}

libtextclassifier3::StatusOr<std::string> QueryVisitor::RetrieveStringValue() {
  if (pending_values_.empty() || !pending_values_.top().holds_text()) {
    return absl_ports::InvalidArgumentError("Unable to retrieve string value.");
  }
  std::string string_value = std::move(pending_values_.top().text);
  pending_values_.pop();
  return string_value;
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
QueryVisitor::RetrieveIterator() {
  if (pending_values_.top().holds_iterator()) {
    std::unique_ptr<DocHitInfoIterator> iterator =
        std::move(pending_values_.top().iterator);
    pending_values_.pop();
    return iterator;
  }
  ICING_ASSIGN_OR_RETURN(std::string value, RetrieveStringValue());
  // Make it into a term iterator.
  return index_.GetIterator(value, kSectionIdMaskAll, match_type_,
                            /*need_term_hit_frequency_=*/false);
}

libtextclassifier3::StatusOr<std::vector<std::unique_ptr<DocHitInfoIterator>>>
QueryVisitor::RetrieveIterators() {
  std::vector<std::unique_ptr<DocHitInfoIterator>> iterators;
  while (!pending_values_.empty() && !pending_values_.top().is_placeholder()) {
    ICING_ASSIGN_OR_RETURN(std::unique_ptr<DocHitInfoIterator> itr,
                           RetrieveIterator());
    iterators.push_back(std::move(itr));
  }
  if (pending_values_.empty()) {
    return absl_ports::InvalidArgumentError(
        "Unable to retrieve expected iterators.");
  }
  return iterators;
}

libtextclassifier3::StatusOr<QueryVisitor::PendingValue>
QueryVisitor::ProcessNumericComparator(const NaryOperatorNode* node) {
  // 1. The children should have been processed and added their outputs to
  // pending_values_. Time to process them.
  // The first two pending values should be the int value and the property.
  ICING_ASSIGN_OR_RETURN(int64_t int_value, RetrieveIntValue());
  ICING_ASSIGN_OR_RETURN(std::string property, RetrieveStringValue());

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
      RetrieveIterators());
  return PendingValue(CreateAndIterator(std::move(iterators)));
}

libtextclassifier3::StatusOr<QueryVisitor::PendingValue>
QueryVisitor::ProcessOrOperator(const NaryOperatorNode* node) {
  ICING_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<DocHitInfoIterator>> iterators,
      RetrieveIterators());
  return PendingValue(CreateOrIterator(std::move(iterators)));
}

libtextclassifier3::StatusOr<QueryVisitor::PendingValue>
QueryVisitor::ProcessHasOperator(const NaryOperatorNode* node) {
  // 1. The children should have been processed and added their outputs to
  // pending_values_. Time to process them.
  // The first two pending values should be the delegate and the property.
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<DocHitInfoIterator> delegate,
                         RetrieveIterator());
  // TODO(b/208654892): The HAS operator need to be able to differentiate
  // between values that came from STRING nodes and those that came from
  // members. members should be allowed as the left operator to HAS, but STRINGs
  // should not be. IOW, `"prop1":foo` should not be treated equivalently to
  // `prop1:foo`
  ICING_ASSIGN_OR_RETURN(std::string property, RetrieveStringValue());
  return PendingValue(std::make_unique<DocHitInfoIteratorSectionRestrict>(
      std::move(delegate), &document_store_, &schema_store_,
      std::move(property)));
}

void QueryVisitor::VisitFunctionName(const FunctionNameNode* node) {
  pending_error_ = absl_ports::UnimplementedError(
      "Function Name node visiting not implemented yet.");
}

void QueryVisitor::VisitString(const StringNode* node) {
  auto escaped_string_or = EscapeStringValue(node->value());
  if (!escaped_string_or.ok()) {
    pending_error_ = std::move(escaped_string_or).status();
    return;
  }
  features_.insert(kVerbatimSearchFeature);
  std::string escaped_string = std::move(escaped_string_or).ValueOrDie();
  pending_values_.push(PendingValue(std::move(escaped_string)));
}

void QueryVisitor::VisitText(const TextNode* node) {
  // TODO(b/208654892): Add support for 1. segmentation and 2. the prefix
  // prefix operator (*).
  std::string normalized_text = normalizer_.NormalizeTerm(node->value());
  pending_values_.push(PendingValue(std::move(normalized_text)));
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
  std::string member = std::move(pending_values_.top().text);
  pending_values_.pop();
  while (!pending_values_.empty() && !pending_values_.top().is_placeholder()) {
    member = absl_ports::StrCat(pending_values_.top().text, kPropertySeparator,
                                member);
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

  pending_values_.push(PendingValue(std::move(member)));
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

  // 1. Put in a placeholder PendingValue
  pending_values_.push(PendingValue());

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
  auto iterator_or = RetrieveIterator();
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
  for (const std::unique_ptr<Node>& child : node->children()) {
    child->Accept(this);
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
  } else if (node->operator_text() == ":") {
    pending_value_or = ProcessHasOperator(node);
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

}  // namespace lib
}  // namespace icing
