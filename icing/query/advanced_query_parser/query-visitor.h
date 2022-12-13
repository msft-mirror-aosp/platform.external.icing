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

#ifndef ICING_QUERY_ADVANCED_QUERY_PARSER_QUERY_VISITOR_H_
#define ICING_QUERY_ADVANCED_QUERY_PARSER_QUERY_VISITOR_H_

#include <cstdint>
#include <memory>
#include <stack>
#include <string>

#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"

namespace icing {
namespace lib {

// The Visitor used to create the DocHitInfoIterator tree from the AST output by
// the parser.
class QueryVisitor : public AbstractSyntaxTreeVisitor {
 public:
  explicit QueryVisitor(const NumericIndex<int64_t>* numeric_index)
      : numeric_index_(*numeric_index) {}

  void VisitFunctionName(const FunctionNameNode* node) override;
  void VisitString(const StringNode* node) override;
  void VisitText(const TextNode* node) override;
  void VisitMember(const MemberNode* node) override;
  void VisitFunction(const FunctionNode* node) override;
  void VisitUnaryOperator(const UnaryOperatorNode* node) override;
  void VisitNaryOperator(const NaryOperatorNode* node) override;

  // RETURNS:
  //   - the DocHitInfoIterator that is the root of the query iterator tree
  //   - INVALID_ARGUMENT if the AST does not conform to supported expressions
  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>> root() && {
    if (has_pending_error()) {
      return pending_error_;
    }
    if (pending_values_.size() != 1 ||
        !pending_values_.top().holds_iterator()) {
      return absl_ports::InvalidArgumentError(
          "Visitor does not contain a single root iterator.");
    }
    return std::move(pending_values_.top().iterator);
  }

 private:
  // A holder for intermediate results when processing child nodes.
  struct PendingValue {
    PendingValue() = default;

    explicit PendingValue(std::unique_ptr<DocHitInfoIterator> iterator)
        : iterator(std::move(iterator)) {}

    explicit PendingValue(std::string text) : text(std::move(text)) {}

    // Placeholder is used to indicate where the children of a particular node
    // begin.
    bool is_placeholder() const { return iterator == nullptr && text.empty(); }

    bool holds_text() const { return iterator == nullptr && !text.empty(); }

    bool holds_iterator() const { return iterator != nullptr && text.empty(); }

    std::unique_ptr<DocHitInfoIterator> iterator;
    std::string text;
  };

  bool has_pending_error() const { return !pending_error_.ok(); }

  // Processes the PendingValue at the top of pending_values_, parses it into a
  // int64_t and pops the top.
  // Returns:
  //   - On success, the int value stored in the text at the top
  //   - INVALID_ARGUMENT if pending_values_ is empty, doesn't hold a text or
  //     can't be parsed as an int.
  libtextclassifier3::StatusOr<int64_t> RetrieveIntValue();

  // Processes the PendingValue at the top of pending_values_ and pops the top.
  // Returns:
  //   - On success, the string value stored in the text at the top
  //   - INVALID_ARGUMENT if pending_values_ is empty or doesn't hold a text.
  libtextclassifier3::StatusOr<std::string> RetrieveStringValue();

  // Processes the NumericComparator represented by node. This must be called
  // *after* this node's children have been visited. The PendingValues added by
  // this node's children will be consumed by this function and the PendingValue
  // for this node will be returned.
  // Returns:
  //   - On success, then PendingValue representing this node and it's children.
  //   - INVALID_ARGUMENT if unable to retrieve string value or int value
  //   - NOT_FOUND if there is no entry in the numeric index for the property
  libtextclassifier3::StatusOr<PendingValue> ProcessNumericComparator(
      const NaryOperatorNode* node);

  std::stack<PendingValue> pending_values_;
  libtextclassifier3::Status pending_error_;

  const NumericIndex<int64>& numeric_index_;  // Does not own!
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_QUERY_ADVANCED_QUERY_PARSER_QUERY_VISITOR_H_
