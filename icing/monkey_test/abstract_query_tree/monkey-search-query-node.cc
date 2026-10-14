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

#include "icing/monkey_test/abstract_query_tree/monkey-search-query-node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

namespace {

std::string Escape(const std::string& query) {
  std::string result;
  for (char c : query) {
    if (c == '"' || c == '\\') {
      result += '\\';
    }
    result += c;
  }
  return result;
}

}  // namespace

MonkeySearchQueryNode::MonkeySearchQueryNode(
    std::unique_ptr<MonkeyAbstractRestrictableLeafQueryNode> subquery,
    std::vector<std::string> property_paths)
    : subquery_(std::move(subquery)),
      property_paths_(std::move(property_paths)) {}

// Unlike other versions of EvaluateQuery, this EvaluateQuery will modify the
// subquery by adding property restricts to it. This is because we assume that
// the query tree will only be used once, so the subquery will not be
// re-evaluated again.
libtextclassifier3::StatusOr<std::vector<DocumentId>>
MonkeySearchQueryNode::EvaluateQuery(
    const InMemoryIcingSearchEngine* engine) const {
  if (!property_paths_.empty()) {
    subquery_->AddPropertyRestricts(property_paths_);
  }
  return subquery_->EvaluateQuery(engine);
}

std::string MonkeySearchQueryNode::GenerateQueryString() const {
  std::string result = absl_ports::StrCat(
      "search(\"", Escape(subquery_->GenerateQueryString()), "\"");
  if (!property_paths_.empty()) {
    absl_ports::StrAppend(&result, ", createList(");
    for (size_t i = 0; i < property_paths_.size(); ++i) {
      absl_ports::StrAppend(&result, "\"", property_paths_[i], "\"");
      if (i < property_paths_.size() - 1) {
        absl_ports::StrAppend(&result, ", ");
      }
    }
    absl_ports::StrAppend(&result, ")");
  }
  absl_ports::StrAppend(&result, ")");
  return result;
}

}  // namespace lib
}  // namespace icing
