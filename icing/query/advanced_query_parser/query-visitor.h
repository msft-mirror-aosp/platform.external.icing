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
#include <unordered_set>

#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-store.h"
#include "icing/transform/normalizer.h"
#include "icing/query/query-features.h"

namespace icing {
namespace lib {

// The Visitor used to create the DocHitInfoIterator tree from the AST output by
// the parser.
class QueryVisitor : public AbstractSyntaxTreeVisitor {
 public:
  explicit QueryVisitor(Index* index,
                        const NumericIndex<int64_t>* numeric_index,
                        const DocumentStore* document_store,
                        const SchemaStore* schema_store,
                        const Normalizer* normalizer,
                        TermMatchType::Code match_type)
      : index_(*index),
        numeric_index_(*numeric_index),
        document_store_(*document_store),
        schema_store_(*schema_store),
        normalizer_(*normalizer),
        match_type_(match_type) {}

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
  //   - NOT_FOUND if the AST refers to a property that does not exist
  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>> root() && {
    if (has_pending_error()) {
      return pending_error_;
    }
    if (pending_values_.size() != 1) {
      return absl_ports::InvalidArgumentError(
          "Visitor does not contain a single root iterator.");
    }
    auto iterator_or = RetrieveIterator();
    if (!iterator_or.ok()) {
      pending_error_ = std::move(iterator_or).status();
      return pending_error_;
    }
    return std::move(iterator_or).ValueOrDie();
  }

  // Returns the set of features used in the query.
  const std::unordered_set<Feature>& features() const { return features_; }

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

  // Processes the PendingValue at the top of pending_values_ and pops the top.
  // Returns:
  //   - On success, a DocHitInfoIterator representing for the term at the top
  //   - INVALID_ARGUMENT if pending_values_ is empty or if unable to create an
  //       iterator for the term.
  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
  RetrieveIterator();

  // Processes all PendingValues at the top of pending_values_ until the first
  // placeholder is encounter.
  // Returns:
  //   - On success, a vector containing all DocHitInfoIterators representing
  //     the values at the top of pending_values_
  //   - INVALID_ARGUMENT if pending_values_is empty or if unable to create an
  //       iterator for any of the terms at the top of pending_values_
  libtextclassifier3::StatusOr<std::vector<std::unique_ptr<DocHitInfoIterator>>>
  RetrieveIterators();

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

  // Processes the AND and OR operators represented by the node. This must be
  // called *after* this node's children have been visited. The PendingValues
  // added by this node's children will be consumed by this function and the
  // PendingValue for this node will be returned.
  // Returns:
  //   - On success, then PendingValue representing this node and it's children.
  //   - INVALID_ARGUMENT if unable to retrieve iterators for any of this node's
  //       children.
  libtextclassifier3::StatusOr<PendingValue> ProcessAndOperator(
      const NaryOperatorNode* node);

  // Processes the OR operator represented by the node. This must be called
  // *after* this node's children have been visited. The PendingValues added by
  // this node's children will be consumed by this function and the PendingValue
  // for this node will be returned.
  // Returns:
  //   - On success, then PendingValue representing this node and it's children.
  //   - INVALID_ARGUMENT if unable to retrieve iterators for any of this node's
  //       children.
  libtextclassifier3::StatusOr<PendingValue> ProcessOrOperator(
      const NaryOperatorNode* node);

  // Processes the HAS operator represented by the node. This must be called
  // *after* this node's children have been visited. The PendingValues added by
  // this node's children will be consumed by this function and the PendingValue
  // for this node will be returned.
  // Returns:
  //   - On success, then PendingValue representing this node and it's children.
  //   - INVALID_ARGUMENT if unable to properly retrieve an iterator
  //       representing the second child
  libtextclassifier3::StatusOr<PendingValue> ProcessHasOperator(
      const NaryOperatorNode* node);

  std::stack<PendingValue> pending_values_;
  libtextclassifier3::Status pending_error_;

  Index& index_;                                // Does not own!
  const NumericIndex<int64_t>& numeric_index_;  // Does not own!
  const DocumentStore& document_store_;         // Does not own!
  const SchemaStore& schema_store_;             // Does not own!
  const Normalizer& normalizer_;                // Does not own!
  TermMatchType::Code match_type_;
  // Set of features invoked in the query.
  std::unordered_set<Feature> features_;

};

}  // namespace lib
}  // namespace icing

#endif  // ICING_QUERY_ADVANCED_QUERY_PARSER_QUERY_VISITOR_H_
