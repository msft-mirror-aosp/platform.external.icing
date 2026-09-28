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

#include "icing/monkey_test/abstract_query_tree/monkey-has-property-query-node.h"

#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"

namespace icing {
namespace lib {

MonkeyHasPropertyQueryNode::MonkeyHasPropertyQueryNode(
    std::string property_path, std::vector<std::string> document_namespaces,
    std::vector<std::string> document_schema_types)
    : MonkeyAbstractLeafQueryNode(std::move(document_namespaces),
                                  std::move(document_schema_types)),
      property_path_(std::move(property_path)) {}

libtextclassifier3::StatusOr<bool>
MonkeyHasPropertyQueryNode::DoesDocumentMatchQuery(
    const InMemoryIcingSearchEngine* engine,
    const MonkeyTokenizedDocument& document) const {
  // A document has a property if it has a section with the given property path
  // and at least one non-empty value.
  for (const auto& section : document.sections) {
    if (section.path == property_path_) {
      bool has_string_value = false;
      for (const std::string& value : section.string_values) {
        // Empty string ("") are not considered non-empty and thus we need
        // to check if there are any non-empty string values.
        if (!value.empty()) {
          has_string_value = true;
          break;
        }
      }
      return has_string_value || !section.integer_values.empty() ||
             !section.vector_values.empty();
    }
  }
  return false;
}

std::string MonkeyHasPropertyQueryNode::GenerateQueryString() const {
  return absl_ports::StrCat("hasProperty(\"", property_path_, "\")");
}

}  // namespace lib
}  // namespace icing
