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

#include "icing/monkey_test/abstract_query_tree/monkey-numeric-query-node.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/schema.pb.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// Static helper to convert NumericComparator enum to string operator.
/* static */ std::string MonkeyNumericQueryNode::ComparatorToString(
    NumericComparator comparator) {
  switch (comparator) {
    case NumericComparator::kEqual:
      return "==";
    case NumericComparator::kNotEqual:
      return "!=";
    case NumericComparator::kLessThan:
      return "<";
    case NumericComparator::kLessThanEqual:
      return "<=";
    case NumericComparator::kGreaterThan:
      return ">";
    case NumericComparator::kGreaterThanEqual:
      return ">=";
    case NumericComparator::kUnknown:
      return "";
  }
}

MonkeyNumericQueryNode::MonkeyNumericQueryNode(std::string property_path,
                                               NumericComparator comparator,
                                               int64_t value)
    : MonkeyNumericQueryNode(std::move(property_path), comparator, value,
                             /*document_namespaces=*/{},
                             /*document_schema_types=*/{}) {}

MonkeyNumericQueryNode::MonkeyNumericQueryNode(
    std::string property_path, NumericComparator comparator, int64_t value,
    std::vector<std::string> document_namespaces,
    std::vector<std::string> document_schema_types)
    : MonkeyAbstractLeafQueryNode(std::move(document_namespaces),
                                  std::move(document_schema_types)),
      property_path_(std::move(property_path)),
      comparator_(comparator),
      value_(value) {}

std::string MonkeyNumericQueryNode::GenerateQueryString() const {
  if (comparator_ == NumericComparator::kUnknown) {
    return "";  // Or handle as an error in test generation
  }
  return absl_ports::StrCat(property_path_, ComparatorToString(comparator_),
                            std::to_string(value_));
}

libtextclassifier3::StatusOr<bool>
MonkeyNumericQueryNode::DoesDocumentMatchQuery(
    const InMemoryIcingSearchEngine* search_engine,
    const MonkeyTokenizedDocument& document) const {
  // Find the section corresponding to property_path_.
  for (const MonkeySection& section : document.sections) {
    if (section.path != property_path_) {
      continue;
    }
    ICING_ASSIGN_OR_RETURN(
        InMemoryIcingSearchEngine::PropertyIndexInfo property_index_info,
        search_engine->GetPropertyIndexInfo(document.document.schema(),
                                            section.path));

    if (property_index_info.numeric_match_type !=
        IntegerIndexingConfig::NumericMatchType::RANGE) {
      // If the property is not configured for numeric indexing (e.g.,
      // UNKNOWN), this numeric query node should not match, even if values
      // exist.
      return false;
    }

    // Perform the numeric comparison for each integer value in the section.
    for (int64_t document_value : section.integer_values) {
      bool match = false;
      switch (comparator_) {
        case NumericComparator::kEqual:
          match = (document_value == value_);
          break;
        case NumericComparator::kNotEqual:
          match = (document_value != value_);
          break;
        case NumericComparator::kLessThan:
          match = (document_value < value_);
          break;
        case NumericComparator::kLessThanEqual:
          match = (document_value <= value_);
          break;
        case NumericComparator::kGreaterThan:
          match = (document_value > value_);
          break;
        case NumericComparator::kGreaterThanEqual:
          match = (document_value >= value_);
          break;
        case NumericComparator::kUnknown:
          // Should not be reached due to GenerateQueryString check,
          // but handle all enum cases.
          break;
      }
      if (match) {
        // If any numeric value in the section satisfies the operator, we can
        // return true immediately.
        return true;  // Found at least one matching value.
      }
    }
  }
  // Property path not found in the document's sections.
  return false;
}

}  // namespace lib
}  // namespace icing
