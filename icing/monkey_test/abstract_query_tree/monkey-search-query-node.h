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

#ifndef ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_SEARCH_QUERY_NODE_H_
#define ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_SEARCH_QUERY_NODE_H_

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// MonkeyNode that represents the search function.
//
// The search function takes a subquery and an optional list of property paths
// and acts like an OR over all the property paths.
// search("foo bar", createList("subject", "property")) is equivalent to the
// query subject:(foo bar) OR property:(foo bar).
class MonkeySearchQueryNode : public MonkeyAbstractQueryNode {
 public:
  explicit MonkeySearchQueryNode(
      std::unique_ptr<MonkeyAbstractRestrictableLeafQueryNode> subquery,
      std::vector<std::string> property_paths = {});

  libtextclassifier3::StatusOr<std::vector<DocumentId>> EvaluateQuery(
      const InMemoryIcingSearchEngine* engine) const override;

  std::string GenerateQueryString() const override;

 private:
  std::unique_ptr<MonkeyAbstractRestrictableLeafQueryNode> subquery_;
  std::vector<std::string> property_paths_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_SEARCH_QUERY_NODE_H_
