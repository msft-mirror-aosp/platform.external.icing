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

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/schema/section-manager.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

bool IsNumericComparator(std::string_view operator_text) {
  if (operator_text.length() < 1 || operator_text.length() > 2) {
    return false;
  }
  // TODO(tjbarron) decide how/if to support !=
  return operator_text == "<" || operator_text == ">" ||
         operator_text == "==" || operator_text == "<=" ||
         operator_text == ">=";
}

bool IsSupportedOperator(std::string_view operator_text) {
  return IsNumericComparator(operator_text);
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
  std::unique_ptr<DocHitInfoIterator> iterator =
      std::move(iterator_or).ValueOrDie();
  return PendingValue(std::move(iterator));
}

void QueryVisitor::VisitFunctionName(const FunctionNameNode* node) {
  pending_error_ = absl_ports::UnimplementedError(
      "Function Name node visiting not implemented yet.");
}

void QueryVisitor::VisitString(const StringNode* node) {
  pending_error_ = absl_ports::UnimplementedError(
      "String node visiting not implemented yet.");
}

void QueryVisitor::VisitText(const TextNode* node) {
  pending_values_.push(PendingValue(node->value()));
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
  pending_error_ =
      absl_ports::UnimplementedError("Not node visiting not implemented yet.");
}

void QueryVisitor::VisitNaryOperator(const NaryOperatorNode* node) {
  if (has_pending_error()) {
    return;
  }

  if (!IsSupportedOperator(node->operator_text())) {
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
  PendingValue pending_value;
  if (IsNumericComparator(node->operator_text())) {
    auto pending_value_or = ProcessNumericComparator(node);
    if (!pending_value_or.ok()) {
      pending_error_ = std::move(pending_value_or).status();
      return;
    }
    pending_value = std::move(pending_value_or).ValueOrDie();
  }

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
