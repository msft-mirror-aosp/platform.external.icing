// Copyright (C) 2026 Google LLC
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

#ifndef ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_NUMERIC_QUERY_NODE_H_
#define ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_NUMERIC_QUERY_NODE_H_

#include <cstdint>
#include <string>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"

namespace icing {
namespace lib {

// Represents a numeric comparison query in the Icing Monkey Test AST.
// This node allows testing queries like "price >= 100" or "timestamp <
// 1700000000". It inherits from MonkeyAbstractLeafQueryNode to support
// filtering by namespaces and schema types before performing the numeric
// comparison.
class MonkeyNumericQueryNode : public MonkeyAbstractLeafQueryNode {
 public:
  // Defines the types of numeric comparisons supported. These map to Icing's
  // numeric query syntax.
  enum class NumericComparator {
    kUnknown,
    kEqual,             // ==
    kNotEqual,          // !=
    kLessThan,          // <
    kLessThanEqual,     // <=
    kGreaterThan,       // >
    kGreaterThanEqual,  // >=
  };

  // Constructs a MonkeyNumericQueryNode.
  // property_path: The path to the INT64 property to query (e.g., "price").
  // comparator: The type of numeric comparison to perform.
  // value: The int64_t value to compare against.
  explicit MonkeyNumericQueryNode(std::string property_path,
                                  NumericComparator comparator, int64_t value);

  // Constructs a MonkeyNumericQueryNode with document filtering.
  explicit MonkeyNumericQueryNode(
      std::string property_path, NumericComparator comparator, int64_t value,
      std::vector<std::string> document_namespaces,
      std::vector<std::string> document_schema_types);

  // Generates the Icing query string for this numeric query.
  // E.g., "price > 100".
  std::string GenerateQueryString() const override;

 private:
  // Returns true if any of the integer_values in the specified property_path
  // within the document satisfy the numeric comparison.
  libtextclassifier3::StatusOr<bool> DoesDocumentMatchQuery(
      const InMemoryIcingSearchEngine* search_engine,
      const MonkeyTokenizedDocument& document) const override;

  // Converts the NumericComparator enum to its string representation used
  // in the Icing query syntax.
  static std::string ComparatorToString(NumericComparator comparator);

  // The path of the property containing the numeric values (e.g., "price").
  std::string property_path_;

  // The type of numeric comparison to perform.
  NumericComparator comparator_;

  // The int64_t value used in the comparison.
  int64_t value_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_NUMERIC_QUERY_NODE_H_
